
import os
import pandas as pd 
import numpy as np
import itertools
import calendar
import baseflow 
import contextlib 
import io
import warnings

from abc import ABC, abstractmethod
from typing import Optional, Dict, List

class TSValidator:

    def __init__(self, 
                 data: pd.DataFrame, 
                 data_columns: List[str],
                 freq: str,
                 min_tot_years: Optional[int] = None,
                 min_consecutive_years: Optional[int] = None,
                 min_availability: Optional[float] = None):
        self.data = data  # Expect data to be formatted with datetime index and single/multi column
        self.data_columns = data_columns
        self.freq = freq
        self.criteria: Dict[str, Optional[float | int]] = {}
        self._set_validity_criteria(min_tot_years, min_consecutive_years, min_availability)
        self._compute_annual_availability()

    def _set_validity_criteria(self,
                               min_tot_years: Optional[int],
                               min_consecutive_years: Optional[int],
                               min_availability: Optional[float]):

        new_values = {
            'min_tot_years': min_tot_years,
            'min_consecutive_years': min_consecutive_years,
            'min_availability': min_availability
        }

        # Only store non-None values
        self.criteria = {k: v for k, v in new_values.items() if v is not None}

        # Check consistency
        if ((self.criteria.get('min_tot_years') is not None or self.criteria.get('min_consecutive_years') is not None)
            and self.criteria.get('min_availability') is None):
            raise ValueError(
                "`min_availability` must be specified when using `min_tot_years` or `min_consecutive_years`"
            )

    def update_criteria(self, **kwargs):
        self._set_validity_criteria(
            kwargs.get('min_tot_years', self.criteria.get('min_tot_years')),
            kwargs.get('min_consecutive_years', self.criteria.get('min_consecutive_years')),
            kwargs.get('min_availability', self.criteria.get('min_availability'))
        )
        return self

    @property 
    def valid_years(self): 
        if 'min_availability' in self.criteria: 
            valid = self.availability >= self.criteria['min_availability']
            return valid[valid].index 
        else: 
            return None 

    def _compute_annual_availability(self) -> pd.Series:
        """Compute fraction of non-NaN values per year."""
        df = self.data.copy()
        expected = df['water_year'].value_counts().mode()[0]
        expected = max(expected, 365) # Just in case of time series with less than one year of data
        df = df.dropna(subset=self.data_columns)
        annual_counts = df.groupby('water_year').count().iloc[:, 0]
        self.availability = annual_counts / expected

    def _get_n_tot_years(self) -> Optional[int]:
        if 'min_availability' in self.criteria:
            return (self.availability >= self.criteria['min_availability']).sum()
        return None

    def _get_n_consecutive_years(self) -> Optional[int]:
        if 'min_availability' in self.criteria:
            valid_years = self.availability[self.availability >= self.criteria['min_availability']]
            if valid_years.empty:
                return 0
            sorted_years = valid_years.index.sort_values()
            diffs = sorted_years.to_series().diff().fillna(1)
            group = (diffs != 1).cumsum()
            return group.value_counts().max()
        return None

    def is_valid(self) -> Optional[bool]:
        checks = []

        if 'min_tot_years' in self.criteria:
            checks.append(self._get_n_tot_years() >= self.criteria['min_tot_years'])

        if 'min_consecutive_years' in self.criteria:
            checks.append(self._get_n_consecutive_years() >= self.criteria['min_consecutive_years'])

        if checks:
            return all(checks)
        return None

    def summary(self) -> dict:
        return {
            "min_tot_years": self.criteria.get('min_tot_years'),
            "actual_tot_years": self._get_n_tot_years(),
            "min_consecutive_years": self.criteria.get('min_consecutive_years'),
            "actual_consecutive_years": self._get_n_consecutive_years(),
            "min_availability": self.criteria.get('min_availability')
        }


class HydroTS: 

    TIME_COLUMN = 'time'
    VARIABLE_COLUMNS = ['Q', 'H', 'T2M']
    COLUMN_ALIASES = {
        'date': 'time',
        'discharge': 'Q',
        'flow': 'Q',
        'flow_cumecs': 'Q'
    }

    def __init__(self, 
                 data: pd.DataFrame, 
                 metadata: pd.DataFrame,
                 freq: str, 
                 use_water_year: bool = True,
                 use_local_water_year: bool = True):

        self.freq = freq 
        self.data = self._format_data(data, use_local_water_year, use_water_year) 
        self.metadata = self._format_metadata(metadata)
        self.validator = TSValidator(self.data, self.data_columns, self.freq)

    def update_validity_criteria(self, **kwargs): 
        self.validator.update_criteria(**kwargs)

    def update_intermittency_criteria(self, min_zero_flow_days: int = 5, min_zero_flow_years: int = 1): 
        if min_zero_flow_years > self.n_years: 
            warnings.warn(f'`min_zero_flow_years` exceeds total number of valid years (={self.n_years}): setting value to {self.n_years}')
            min_zero_flow_years = self.n_years 
        self.intermittency_criteria = {'min_zero_flow_days': min_zero_flow_days, 'min_zero_flow_years': min_zero_flow_years}

    @property 
    def data_columns(self): 
        return [col for col in self.data.columns if col in self.VARIABLE_COLUMNS]

    @property
    def is_valid(self):
        """Check if dataframe has the minimum number of years at the specified availability."""
        return self.validator.is_valid()

    def _format_data(self, data: pd.DataFrame, use_water_year: bool, use_local_water_year: bool):
        data = self._standardize_columns(data)
        data = self._validate_columns(data)
        
        # Ensure time column is properly formatted
        data['time'] = pd.to_datetime(data['time'])
        data = data.sort_values(by='time')
        data = data.set_index('time')
        data.index.name = 'time'

        if use_water_year:
            self.water_year_start = self._get_water_year_start(data, use_local_water_year)
        else:
            self.water_year_start = (1, 1) # Jan 1 

        data = self._make_continuous(data)
        data = self._assign_water_year(data)
        return data 

    def _standardize_columns(self, data: pd.DataFrame):
        """Replace known aliases of standard column names."""
        return data.rename(columns={k: v for k, v in self.COLUMN_ALIASES.items() if k in data.columns})
    
    def _validate_columns(self, data: pd.DataFrame):
        missing_time = self.TIME_COLUMN if self.TIME_COLUMN not in data.columns else None
        missing_variable = False
        messages = []
        if missing_time:
            messages.append(f"Missing time column: '{missing_time}'")

        if missing_variable:
            messages.append(f"Missing variable column(s)")

        if messages:
            raise ValueError("\n".join(messages))

        valid_columns = [col for col in data.columns if col in [self.TIME_COLUMN] + self.VARIABLE_COLUMNS]
        data = data[valid_columns]
        return data 

    def _make_continuous(self, data: pd.DataFrame):
        """Enforce a continuous time series at the specified resolution."""
        start = data.index[0]
        end = data.index[-1]
        full_range = pd.date_range(start=start, end=end, freq=self.freq)
        data = data.reindex(full_range)
        data.index.name = 'time'
        return data

    def _format_metadata(self, metadata: pd.DataFrame): 
        return metadata 

    def _impute_missing_values(self): 
        """Impute missing data in timeseries."""
        raise NotImplementedError()

    def _compute_availability(self):
        """Compute the data availability in each year."""
        avail = self.data.groupby(self.data['water_year']).apply(lambda g: g['Q'].notna().sum() / g.shape[0], include_groups=False)
        avail = avail.sort_index()
        return avail

    def _get_local_water_year_start(self, data: pd.DataFrame, wettest: bool = True):
        """Compute the start of the local water year.

        If `wettest=True` then we assume the water year starts in the 
        first month after the highest mean monthly flow. This is 
        appropriate for computing low-flow statistics. On the other 
        hand, if `wettest=False` then we assume the water year starts 
        in the driest month on average. This is more appropriate for 
        computing high-flow statistics.  
        """
        qmean = data.groupby(data.index.month)['Q'].mean()
        if wettest: 
            # Assume water year starts in the first month after the highest mean monthly flow
            # https://doi.org/10.1029/2023WR035063 
            water_year_start_month = (int(qmean.idxmax())) % 12 + 1
        else:
            # Assume water year starts in the driest month
            water_year_start_month = int(qmean.idxmin())
        return (water_year_start_month, 1)

    def _get_water_year_start(self, data: pd.DataFrame, use_local_water_year: bool, **kwargs): 
        if use_local_water_year:
            return self._get_local_water_year_start(data, **kwargs)
        else:
            if self.metadata['latitude'] > 0:
                # Northern hemisphere starts Oct 1
                return (10, 1)
            else:
                # Southern hemisphere starts Apr 1
                return (4, 1)

    def _assign_water_year(self, data: pd.DataFrame):
        """Assign water year to timeseries dataset."""
        def get_water_year(dt, start_month, start_day):
            if (dt.month, dt.day) >= (start_month, start_day):
                return dt.year
            else:
                return dt.year - 1
        wy = pd.Series(data.index).apply(lambda x: get_water_year(x, *self.water_year_start))
        data['water_year'] = wy.values
        return data

    @property
    def is_intermittent(self):
        """
        Check the streamflow data to see if the catchment is intermittent.

        A timeseries is considered intermittent if it has, on average, more than
        `min_zero_flow_days` days of zero flow per year and if at least a 
        `min_zero_flow_years_frac` fraction of years containing one or more zero-flow days.

        Returns:
            bool: True if the timeseries is intermittent, False otherwise.
        """

        min_zero_flow_days = self.intermittency_criteria['min_zero_flow_days']
        min_zero_flow_years = self.intermittency_criteria['min_zero_flow_years']
        if min_zero_flow_days is None or min_zero_flow_years is None:
            return None

        if min_zero_flow_years > self.n_years: 
            warnings.warn(f'`min_zero_flow_years` exceeds total number of valid years (={self.n_years}): setting value to {self.n_years}')
            min_zero_flow_years = self.n_years 
        # Filter the dataframe for available years
        data = self.data[self.data.index.year.isin(self.valid_years)]
        
        # Calculate the number of zero-flow days per year
        n_zero = data.groupby(data['water_year'])['Q'].apply(lambda x: (x < 0.001).sum(), include_groups=False)
        
        # Mean number of zero flow days per year
        mean_n_zero = n_zero.mean()
        
        # Fraction of years that are intermittent
        n_intermittent = (n_zero >= min_zero_flow_days).sum() #/ len(self.valid_years)
 
        # Return True if criteria for intermittent are met
        return (float(mean_n_zero) >= min_zero_flow_days) and (float(n_intermittent) > min_zero_flow_years)

    @property 
    def start(self): 
        return self.data.index[0]
    
    @property 
    def end(self): 
        return self.data.index[-1]

    @property 
    def valid_years(self): 
        return self.validator.valid_years 

    @property 
    def n_years(self):
        return len(self.valid_years)

    @property 
    def summary(self):
        return TSSummary(self)

class TSDynamicModel(ABC):
    def __init__(self):
        pass 

# FIXME this currently makes assumption that timeseries has daily resolution
class TSSummary:
    _custom_summary_functions = {}

    def __init__(self, ts: HydroTS, use_complete_years=True): 
        self.ts = ts 

    def annual_maximum_flow(self) -> pd.DataFrame:
        return self.ts.data.groupby('water_year')[['Q']].max() 

    def n_day_flow_extreme(self, n: int = 7, fun: str = 'min') -> pd.DataFrame:
        df = self.ts.data.copy()
        df = df[df['water_year'].isin(self.ts.valid_years)]
        years = df['water_year'].unique() 
        
        def custom_nanmean(window, n):
            # Check if the number of non-NaN values meets the min_periods requirement
            valid_values = window #[~np.isnan(window)]
            if len(valid_values) >= n:
                return np.nanmean(window)
            else:
                return np.nan  # Return NaN if min_periods is not met

        df_list = []
        for year in years: 
            df_year = df[df['water_year'] == year].copy()
            # This applies np.nanmean to sequences of length n (including nan values), otherwise returning nan
            df_year[f'{fun}_Q_mean'] = df_year['Q'].rolling(str(n) + 'd', min_periods=1).apply(custom_nanmean, args=(n,)).shift(-(n-1))
            if fun == 'min':
                df_year_ext = df_year.loc[[df_year[f'{fun}_Q_mean'].idxmin()]]
            elif fun == 'max':
                df_year_ext = df_year.loc[[df_year[f'{fun}_Q_mean'].idxmax()]]

            df_year_ext = df_year_ext.drop('Q', axis=1)
            df_list.append(df_year_ext)

        out = pd.concat(df_list, axis=0).reset_index()
        out = out[['water_year', f'{fun}_Q_mean', 'time']]
        out = out.rename({
            'time': f'{fun}_Q_mean_{n}d_start_time',
            'min_Q_mean': f'{fun}_Q_mean_{n}d'
        }, axis=1)
        return out

    def n_day_low_flow_extreme(self, n: int = 7) -> pd.DataFrame:
        return self.n_day_flow_extreme(n, 'min')

    def n_day_high_flow_extreme(self, n: int = 7) -> pd.DataFrame: 
        return self.n_day_flow_extreme(n, 'max')

    def max_low_flow_duration(self, quantile: float) -> pd.DataFrame: 
        df = self.ts.data.copy()
        df = df[df['water_year'].isin(self.ts.valid_years)]
        q_str = self._format_quantile(quantile)
        threshold = df['Q'].quantile((1-quantile))
        df['below'] = df['Q'] <= threshold 
        df['below'] = df['below'].astype(int)
        years = df['water_year'].unique()
        df_list = []
        for year in years: 
            df_year = df[df['water_year'] == year].copy()
            # Calculate the lengths of consecutive periods where 'below' == 1
            df_year['below_shifted'] = df_year['below'].shift(1, fill_value=0)
            df_year['period'] = (df_year['below'] != df_year['below_shifted']).cumsum()
            # Filter only the periods where the value is below the threshold
            below_periods = df_year[df_year['below'] == 1].groupby('period').size()
            if not below_periods.empty:
                max_consecutive_days = below_periods.max()
                # Find the start time of the longest below-threshold period
                longest_period = below_periods.idxmax()
                start_of_longest_period = df_year[df_year['period'] == longest_period].index.min()
                df_list.append(pd.DataFrame.from_dict({
                    'water_year': [year],
                    f'longest_period_below_{q_str}': [max_consecutive_days], 
                    f'longest_period_below_{q_str}_start_time': [start_of_longest_period], 
                }))

        out = pd.concat(df_list, axis=0).reset_index(drop=True)
        return out

    def max_low_flow_deficit(self, quantile: float) -> pd.DataFrame:
        df = self.ts.data.copy()
        df = df[df['water_year'].isin(self.ts.valid_years)]
        q_str = self._format_quantile(quantile)
        threshold = df['Q'].quantile((1-quantile))
        df['below'] = df['Q'] <= threshold 
        df['below'] = df['below'].astype(int)
        df['deficit'] = df['Q'].apply(lambda x: threshold - x if x <= threshold else 0)
        years = df['water_year'].unique()
        df_list = []
        for year in years: 
            df_year = df[df['water_year'] == year].copy()
            # Calculate the lengths of consecutive periods where 'below' == 1
            df_year['below_shifted'] = df_year['below'].shift(1, fill_value=0)
            df_year['period'] = (df_year['below'] != df_year['below_shifted']).cumsum()
            # Filter only the periods where the value is below the threshold
            below_periods = df_year[df_year['below'] == 1].groupby('period')['deficit'].sum()
            if not below_periods.empty:
                max_deficit = below_periods.max()
                max_deficit_period = below_periods.idxmax()
                start_of_max_deficit_period = df_year[df_year['period'] == max_deficit_period].index.min()
                df_list.append(pd.DataFrame.from_dict({
                    'water_year': [year],
                    f'max_deficit_below_{q_str}': [max_deficit], 
                    f'max_deficit_below_{q_str}_start_time': [start_of_max_deficit_period]
                }))

        out = pd.concat(df_list, axis=0).reset_index(drop=True)
        return out

    def no_flow_event_duration(self) -> pd.DataFrame: 
        df = self.ts.data.copy()
        df = df[df['water_year'].isin(self.ts.valid_years)]
        
        # Function to calculate the no-flow duration recurrence period
        df['noflow'] = np.where(df['Q'] <= 0.001, 1, 0)
        # rle_no_flow = df['noflow'].values
        rle_no_flow = [(k, len(list(v))) for k, v in itertools.groupby(df['noflow'])]
        event_ids = [[i] * grp[1] for i, grp in enumerate(rle_no_flow)]
        event_ids = list(itertools.chain.from_iterable(event_ids))
        df['event_id'] = event_ids
        df = df.reset_index()
        no_flow_events = df[df['noflow'] == 1].groupby('event_id').agg(
            water_year=('water_year', 'min'), # Take the water year of the event start
            start_time=('time', 'min'),
            end_time=('time', 'max'),
            duration=('time', lambda x: (x.max() - x.min()).days + 1)
        )
        return no_flow_events

    def max_no_flow_event_duration(self) -> pd.DataFrame: 
        no_flow_events = self.no_flow_event_duration() 
        return no_flow_events.groupby('water_year')[['duration']].max()

    def no_flow_frequency(self) -> pd.DataFrame:
        """Function to calculate no flow frequency."""
        df = self.ts.data.copy()
        df = df[df['water_year'].isin(self.ts.valid_years)]
        
        # Function to calculate no-flow frequency
        df['noflow'] = np.where(df['Q'] <= 0.001, 1, 0)
        rle_no_flow = [(k, len(list(v))) for k, v in itertools.groupby(df['noflow'])]
        event_ids = [[i] * grp[1] for i, grp in enumerate(rle_no_flow)]
        event_ids = list(itertools.chain.from_iterable(event_ids))
        df['event_id'] = event_ids
        df = df.reset_index()
        no_flow_events = df[df['noflow'] == 1].groupby(['water_year', 'event_id']).agg(
            start_time=('time', 'min'),
            end_time=('time', 'max'),
            duration=('time', lambda x: (x.max() - x.min()).days + 1)
        )
        yearly_events = no_flow_events.groupby('water_year', as_index=False).size().set_index('water_year')
        yearly_events = yearly_events.reindex(self.ts.valid_years).fillna(0)
        yearly_events = yearly_events.rename(columns={'size': 'n'})[['n']].astype(int)
        return yearly_events

    @staticmethod
    def _circular_statistics(dates, hemisphere="Northern"):
        # Function to compute circular statistics (mean direction and regularity)
        day_of_year = dates.dt.dayofyear
        angles = 2 * np.pi * day_of_year / 365
        mean_cos = np.cos(angles).mean()
        mean_sin = np.sin(angles).mean()
        
        theta = np.arctan2(mean_sin, mean_cos)
        
        if hemisphere == "Southern":
            if any((day_of_year >= 182) & (day_of_year <= 365)):
                theta -= np.pi
            elif any((day_of_year >= 1) & (day_of_year <= 181)):
                theta += np.pi
        
        if theta < 0:
            theta += 2 * np.pi
        
        r = np.sqrt(mean_cos**2 + mean_sin**2)
        return theta, r

    @staticmethod
    def _format_quantile(q): 
        q_int = q * 100
        if not q_int % 1: 
            q_int = 'Q' + str(int(q_int))
        else:
            q_int = 'Q' + '{0:.2f}'.format(q_int).rstrip('0')
        return q_int

    @classmethod
    def register_summary_function(cls, name: str, func):
        """Register a new summary function dynamically."""
        cls._custom_summary_functions[name] = func

    def __getattr__(self, name):
        """Dynamically return callable wrappers for custom summary functions."""
        if name in self._custom_summary_functions:
            # If a registered summary function is accessed, return a callable wrapper
            return self.SummaryFunctionWrapper(self._custom_summary_functions[name], self.ts)
        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    class SummaryFunctionWrapper:
        def __init__(self, func, ts):
            self.func = func
            self.ts = ts
        
        def __call__(self, *args, **kwargs):
            """Allow the wrapper to be called like the original function."""
            # Call the function with self.hydrots as the first argument
            return self.func(self.ts, *args, **kwargs)


# NOT USED:

# # UTIL FUNCTIONS
# def format_quantile(q): 
#     q_int = q * 100
#     if not q_int % 1: 
#         q_int = 'Q' + str(int(q_int))
#     else:
#         q_int = 'Q' + '{0:.2f}'.format(q_int).rstrip('0')
#     return q_int

# def custom_nanmean(window, n):
#     # Check if the number of non-NaN values meets the min_periods requirement
#     valid_values = window #[~np.isnan(window)]
#     if len(valid_values) >= n:
#         return np.nanmean(window)
#     else:
#         return np.nan  # Return NaN if min_periods is not met

# def circular_stats(dates, hemisphere="Northern"):
#     # Function to compute circular statistics (mean direction and regularity)
#     day_of_year = dates.dt.dayofyear
#     angles = 2 * np.pi * day_of_year / 365
#     mean_cos = np.cos(angles).mean()
#     mean_sin = np.sin(angles).mean()
#     theta = np.arctan2(mean_sin, mean_cos)
#     if hemisphere == "Southern":
#         if any((day_of_year >= 182) & (day_of_year <= 365)):
#             theta -= np.pi
#         elif any((day_of_year >= 1) & (day_of_year <= 181)):
#             theta += np.pi
#     if theta < 0:
#         theta += 2 * np.pi
#     r = np.sqrt(mean_cos**2 + mean_sin**2)
#     # return {'theta': theta, 'regularity': r}
#     return theta, r

# def local_water_year(df, wettest=True):
#     df = df.copy()
#     df = df.set_index('date')
#     df = df.rolling('30d', min_periods=7).mean()
#     df = df.reset_index()
#     df['day'] = df['date'].dt.dayofyear
#     df = df.groupby('day')['Q'].mean()
#     df = df.loc[1:365]
#     if wettest:
#         water_year_start = df.idxmax()
#     else:
#         water_year_start = df.idxmin()
#     return water_year_start

# class MaxLowFlowDuration(LowFlowMetric): 
#     def compute(self, quantile=0.9):
#         df = self.data.copy()
#         df = df[df.groupby('water_year').transform('size') >= 365]
#         q_str = format_quantile(quantile)
#         threshold = df['Q'].quantile((1-quantile))
#         df['below'] = df['Q'] <= threshold 
#         df['below'] = df['below'].astype(int)
#         years = df['water_year'].unique()
#         df_list = []
#         for year in years: 
#             df_year = df[df['water_year'] == year].copy()
#             # Calculate the lengths of consecutive periods where 'below' == 1
#             df_year['below_shifted'] = df_year['below'].shift(1, fill_value=0)
#             df_year['period'] = (df_year['below'] != df_year['below_shifted']).cumsum()
#             # Filter only the periods where the value is below the threshold
#             below_periods = df_year[df_year['below'] == 1].groupby('period').size()
#             # If there are any periods, find the maximum length of consecutive days below threshold
#             if not below_periods.empty:
#                 max_consecutive_days = below_periods.max()
#                 # Find the start time of the longest below-threshold period
#                 longest_period = below_periods.idxmax()
#                 start_of_longest_period = df_year[df_year['period'] == longest_period]['time'].min()
#                 df_list.append(pd.DataFrame.from_dict({
#                     'water_year': [year],
#                     'longest_period_below_' + q_str: [max_consecutive_days], 
#                     'longest_period_below_' + q_str + '_start_time': [start_of_longest_period], 
#                 }))

#         out = pd.concat(df_list, axis=0).reset_index(drop=True)
#         return out

# class MaxLowFlowDeficit(LowFlowMetric):
#     def compute(self, quantile=0.9):
#         df = self.data.copy()
#         q_str = format_quantile(quantile)
#         threshold = df['Q'].quantile((1-quantile))
#         df['below'] = df['Q'] <= threshold 
#         df['below'] = df['below'].astype(int)
#         df['deficit'] = df['Q'].apply(lambda x: threshold - x if x <= threshold else 0)
#         df = df[df.groupby('water_year').transform('size') >= 365]
#         years = df['water_year'].unique()
#         df_list = []
#         for year in years: 
#             df_year = df[df['water_year'] == year].copy()
#             # Calculate the lengths of consecutive periods where 'below' == 1
#             df_year['below_shifted'] = df_year['below'].shift(1, fill_value=0)
#             df_year['period'] = (df_year['below'] != df_year['below_shifted']).cumsum()
#             # Filter only the periods where the value is below the threshold
#             below_periods = df_year[df_year['below'] == 1].groupby('period')['deficit'].sum()
#             # If there are any periods, find the maximum length of consecutive days below threshold
#             if not below_periods.empty:
#                 max_deficit = below_periods.max()
#                 # Find the start time of the longest below-threshold period
#                 max_deficit_period = below_periods.idxmax()
#                 start_of_max_deficit_period = df_year[df_year['period'] == max_deficit_period]['time'].min()
#                 df_list.append(pd.DataFrame.from_dict({
#                     'water_year': [year],
#                     'max_deficit_below_' + q_str: [max_deficit], 
#                     'max_deficit_below_' + q_str + '_start_time': [start_of_max_deficit_period]
#                 }))

#         out = pd.concat(df_list, axis=0).reset_index(drop=True)
#         return out

class NoFlowEventDuration(LowFlowMetric):
    def compute(self): 
        df = df.copy()
        # Function to calculate the no-flow duration recurrence period
        df['noflow'] = np.where(df['Q'] <= 0.001, 1, 0)
        # rle_no_flow = df['noflow'].values
        rle_no_flow = [(k, len(list(v))) for k, v in itertools.groupby(df['noflow'])]
        event_ids = [[i] * grp[1] for i, grp in enumerate(rle_no_flow)]
        event_ids = list(itertools.chain.from_iterable(event_ids))
        df['event_id'] = event_ids
        no_flow_events = df[df['noflow'] == 1].groupby('event_id').agg(
            start_time=('time', 'min'),
            end_time=('time', 'max'),
            duration=('time', lambda x: (x.max() - x.min()).days + 1)
        )
        # no_flow_events['end_year'] = no_flow_events['end_time'].dt.year
        # max_durations = no_flow_events.groupby('end_year')['duration'].max()
        # D80 = max_durations.quantile(quantile)
        # return D80
        return no_flow_events

# class NoFlowFrequency(LowFlowMetric):
#     def compute(self): 
#         df = df.copy()
#         # Function to calculate no-flow frequency
#         df['noflow'] = np.where(df['Q'] <= 0.001, 1, 0)
#         rle_no_flow = [(k, len(list(v))) for k, v in itertools.groupby(df['noflow'])]
#         event_ids = [[i] * grp[1] for i, grp in enumerate(rle_no_flow)]
#         event_ids = list(itertools.chain.from_iterable(event_ids))
#         df['event_id'] = event_ids
#         df['year'] = df['time'].dt.year
#         no_flow_events = df[df['noflow'] == 1].groupby(['year', 'event_id']).agg(
#             start_time=('time', 'min'),
#             end_time=('time', 'max'),
#             duration=('time', lambda x: (x.max() - x.min()).days + 1)
#         )
#         yearly_events = no_flow_events.groupby('year').size().reset_index(name='n')
#         all_years = df['year'].drop_duplicates().sort_values()
#         yearly_events = pd.merge(pd.DataFrame({'year': all_years}), yearly_events, how='left', on='year').fillna(0)
#         yearly_events['n'] = yearly_events['n'].astype(int)
#         return yearly_events

# class NoFlowTiming(LowFlowMetric):
#     # Function to calculate timing of no-flow events
#     # TODO not looked in detail at this function
#     def compute(self):
#         df = self.data.copy()
#         df['noflow'] = np.where(df['Q'] <= 0.001, 1, 0)
#         no_flow_dates = df[df['noflow'] == 1]['time']
#         return circular_stats(no_flow_dates)

# class BaseflowIndexDecadal(LowFlowMetric): 
#     # TODO not sure what the precedent is for this method?
#     def compute(self, nyears=10):
#         df = self.data.copy()
#         df_base = baseflow.separation(df[['time', 'Q']].set_index('time'), method = 'LH')
#         df_base = df_base['LH'].reset_index().rename({'Q': 'Qb'}, axis=1)
#         df_base = pd.merge(df, df_base, how='left', on='time')
#         df_base['month'] = df_base['time'].dt.month
#         years = df_base['water_year'].unique()
#         bfi_year_list = []
#         bfi_month_list = []
#         for start_year in years:
#             decade_range = (df_base['water_year'] >= start_year) & (df_base['water_year'] <= start_year + (nyears - 1))
#             df_base_decade = df_base[decade_range].copy()
#             df_base_decade['decade_start_year'] = start_year 
#             bfi_year = df_base_decade.groupby('decade_start_year').apply(lambda x: x['Qb'].sum() / x['Q'].sum(), include_groups=False).reset_index(name='bfi')
#             bfi_month = df_base_decade.groupby(['decade_start_year', 'month']).apply(lambda x: x['Qb'].sum() / x['Q'].sum(), include_groups=False).reset_index(name='bfi')
#             bfi_year_list.append(bfi_year)
#             bfi_month_list.append(bfi_month)

#         bfi_year = pd.concat(bfi_year_list).reset_index(drop=True).rename({'bfi': 'bfi_year'}, axis=1)
#         bfi_month = pd.concat(bfi_month_list).reset_index(drop=True)
#         bfi_month = bfi_month.pivot(index='decade_start_year', columns='month', values='bfi')
#         bfi_month.columns = ['bfi_month_' + str(m) for m in bfi_month.columns]
#         bfi = pd.merge(bfi_year, bfi_month, how='left', on='decade_start_year')
#         bfi = bfi.rename({'decade_start_year': 'water_year'}, axis=1)
#         return bfi

# class SeasonalPredictability(LowFlowMetric):
#     # Function to calculate seasonal predictability of dry periods
#     # TODO find precedent for this approach
#     def compute(self): 
#         df = self.data.copy()
#         df['noflow'] = np.where(df['Q'] <= 0.001, 1, 0)
#         df['month'] = df['time'].dt.month
#         df['year'] = df['time'].dt.year
#         monthly_no_flow = df.groupby('month')['noflow'].mean()
        
#         rolling_sums = []
#         for i in range(12):
#             indices = np.arange(i, i + 6) % 12
#             rolling_sums.append({
#                 'start_month': monthly_no_flow.index[i],
#                 'end_month': monthly_no_flow.index[indices[-1]],
#                 'total_no_flow_freq': monthly_no_flow.iloc[indices].sum()
#             })
        
#         rolling_sums_df = pd.DataFrame(rolling_sums)
#         driest_six_months = rolling_sums_df.loc[rolling_sums_df['total_no_flow_freq'].idxmax()]
#         wettest_six_months = rolling_sums_df.loc[rolling_sums_df['total_no_flow_freq'].idxmin()]
        
#         def average_monthly_zero_flow(df, start_month):
#             months = np.arange(start_month, start_month + 6) % 12 + 1
#             mean_F06 = df[df['month'].isin(months)].groupby('year')['noflow'].mean().mean()
#             return mean_F06
        
#         Fd_dry = average_monthly_zero_flow(df, driest_six_months['start_month'])
#         Fd_wet = average_monthly_zero_flow(df, wettest_six_months['start_month'])
#         SD6 = 1 - Fd_wet / Fd_dry
#         return SD6

# class SeasonalRecessionTimescale(LowFlowMetric):
#     # Function to calculate seasonal recession timescale (tentative)
#     # TODO find precedent for this method 
#     def compute(self):
#         df = self.data.copy()
#         df['day'] = df['date'].dt.dayofyear
#         daily_mean = df.groupby('day')['Q'].mean().reset_index()
#         daily_mean = pd.concat([daily_mean, daily_mean.head(30)], ignore_index=True)
#         daily_mean['Q_30d'] = daily_mean['Q'].rolling(window=30, min_periods=1).mean()
        
#         max_idx = daily_mean['Q_30d'].idxmax()
#         daily_mean = pd.concat([daily_mean[max_idx:], daily_mean[:max_idx]], ignore_index=True)
        
#         min_idx = daily_mean['Q_30d'].idxmin()
#         q50 = daily_mean['Q_30d'].quantile(0.5)
#         q90 = daily_mean['Q_30d'].quantile(0.9)
        
#         Drec = len(daily_mean[(daily_mean['Q_30d'] >= q50) & (daily_mean['Q_30d'] <= q90)])
#         return Drec

# class ConcavityIndex(Metric):
#     # TODO find precedent for this method 
#     def compute(self):
#         df = self.data.copy()
#         # Function to calculate concavity index
#         Q = df['Q'].dropna()
#         q = np.quantile(Q, [0.01, 0.1, 0.99])
#         IC = (q[1] - q[2]) / (q[0] - q[2])
#         return IC

# class BaseflowIndex(Metric): 
#     # TODO per year / rolling?
#     def compute(self, method = 'UKIH'):
#         df = self.data.copy()
#         df = df[['time', 'Q']].set_index('time')
#         with contextlib.redirect_stdout(io.StringIO()):
#             bfi = baseflow.separation(df, method='UKIH')
#         bfi = bfi['UKIH']
#         Qt = df['Q'].sum()
#         Qb = bfi['Q'].sum()
#         bfi = Qb / Qt 
#         return bfi

# class RunoffEventDuration(Metric): 
#     # TODO find precedent for this method
#     def compute(self, n_peaks = 5): 
#         df = self.data.copy()
#         df = df[['time', 'Q']].set_index('time')
#         with contextlib.redirect_stdout(io.StringIO()):
#             bfi = baseflow.separation(df, method='UKIH')
#         bfi = bfi['UKIH']
#         bfi = bfi.fillna(0)
#         bfi = bfi.rename({'Q': 'Qb'}, axis=1)
#         df = pd.merge(df, bfi, left_index=True, right_index=True).reset_index()
#         df['Qs'] = df['Q'] - df['Qb']
#         df['runoff'] = (df['Qs'] >= 0.001).astype(int)
#         rle_runoff = [(k, len(list(v))) for k, v in itertools.groupby(df['runoff'])]
#         event_ids = [[i] * grp[1] for i, grp in enumerate(rle_runoff)]
#         event_ids = list(itertools.chain.from_iterable(event_ids))
#         df['event_id'] = event_ids
#         df['year'] = df['time'].dt.year
#         df = df[df['runoff'] == 1]
#         # Select the most severe runoff events using POT approach
#         df_peak = df.groupby(['event_id'])['Q'].max() 
#         # Calculate the target number of peaks
#         n_years = len(df['time'].dt.year.unique())
#         n_target_peaks = n_years * n_peaks
#         n_target_peaks = min(n_target_peaks, df_peak.shape[0])
#         # Sort the flood maxima in descending order
#         sorted_peaks = df_peak.sort_values(ascending=False).reset_index(drop=True)
#         # Set the threshold to select the top 'target_peaks' maximums
#         threshold_value = sorted_peaks.iloc[n_target_peaks - 1]
#         # Identify the peaks over threshold and limit dataframe to these events
#         event_ids = list(df_peak[df_peak > threshold_value].index)
#         df = df[df['event_id'].isin(event_ids)]
#         # Now compute the event duration
#         runoff_events = df['event_id'].unique()
#         Dr = []
#         for event in runoff_events: 
#             df_event = df[df['event_id'] == event]
#             Q_max_index = df_event['Qs'].idxmax()
#             Q_max = df_event['Qs'].max()
#             df_event = df_event.loc[Q_max_index:]
#             event_duration = len([i for i in range(df_event.shape[0]) if df_event['Qs'].values[i] > (Q_max / 2)])
#             Dr.append(event_duration)

#         return Dr

# # Function to calculate seasonal predictability of dry periods
# def seasonal_predictability_flow(df, n=6):
#     df = df.copy()
#     # df['noflow'] = np.where(df['Q'] <= 0.001, 1, 0)
#     df['month'] = df['date'].dt.month
#     df['year'] = df['date'].dt.year
#     monthly_no_flow = df.groupby('month')['Q'].mean()
#     rolling_sums = []
#     for i in range(12):
#         indices = np.arange(i, i + n) % 12
#         rolling_sums.append({
#             'start_month': monthly_no_flow.index[i],
#             'end_month': monthly_no_flow.index[indices[-1]],
#             'total_no_flow_freq': monthly_no_flow.iloc[indices].sum()
#         })
#     rolling_sums_df = pd.DataFrame(rolling_sums)
#     driest_six_months = rolling_sums_df.loc[rolling_sums_df['total_no_flow_freq'].idxmax()]
#     wettest_six_months = rolling_sums_df.loc[rolling_sums_df['total_no_flow_freq'].idxmin()]
#     def average_monthly_flow(df, start_month):
#         months = np.arange(start_month, start_month + n) % 12 + 1
#         mean_F0 = df[df['month'].isin(months)].groupby('year')['Q'].mean().mean()
#         return mean_F0
#     Fd_dry = average_monthly_flow(df, driest_six_months['start_month'])
#     Fd_wet = average_monthly_flow(df, wettest_six_months['start_month'])
#     SD = 1 - Fd_wet / Fd_dry
#     return SD