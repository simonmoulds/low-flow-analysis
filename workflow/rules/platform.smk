

import platform


current_platform = platform.system()
if current_platform == "Linux":
    r_env_file = 'envs/conda_environment_r_linux.yml'
# elif current_platform == "Darwin":
#     r_env_file = 'envs/conda_environment_r_macos.yml'
else:
    raise NotImplementedError(f'Unsupported plaform: {current_platform}')

py_env_file = 'envs/conda_environment_base.yml'