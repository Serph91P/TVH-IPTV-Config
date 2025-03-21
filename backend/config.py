import asyncio
import base64
import json
import os
import subprocess

import aiofiles
import yaml
from mergedeep import merge


def get_home_dir():
    home_dir = os.environ.get('HOME_DIR')
    if home_dir is None:
        home_dir = os.path.expanduser("~")
    return home_dir


async def is_tvh_process_running_locally():
    process_name = 'tvheadend'
    try:
        process = await asyncio.create_subprocess_exec(
            'pgrep', '-x', process_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            return True
        else:
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def is_tvh_process_running_locally_sync():
    process_name = 'tvheadend'
    try:
        result = subprocess.run(
            ['pgrep', '-x', process_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        if result.returncode == 0:
            return True
        else:
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


async def get_admin_file(directory):
    if os.path.exists(directory) and os.listdir(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                async with aiofiles.open(file_path, 'r') as file:
                    try:
                        contents = await file.read()
                        data = json.loads(contents)
                        if data.get('username') == 'admin':
                            return file_path, data
                    except (json.JSONDecodeError, IOError) as e:
                        print(f"Error processing file {file_path}: {e}")
    return None, None


async def update_accesscontrol_files():
    accesscontrol_path = os.path.join(get_home_dir(), '.tvheadend', 'accesscontrol')
    file_path, data = await  get_admin_file(accesscontrol_path)
    if data:
        data['prefix'] = '0.0.0.0/0,::/0'
        async with aiofiles.open(file_path, 'w') as outfile:
            await outfile.write(json.dumps(data, indent=4))


async def get_local_tvh_proc_admin_password():
    passwd_path = os.path.join(get_home_dir(), '.tvheadend', 'passwd')
    file_path, data = await get_admin_file(passwd_path)
    if data:
        encoded_password = data.get('password2')
        try:
            decoded_password = base64.b64decode(encoded_password).decode('utf-8')
            parts = decoded_password.split('-')
            return parts[2]
        except Exception as e:
            print(f"Error decoding password: {e}")
    return None


def write_yaml(file, data):
    if not os.path.exists(os.path.dirname(file)):
        os.makedirs(os.path.dirname(file))
    with open(file, "w") as outfile:
        yaml.dump(data, outfile, default_flow_style=False)


def read_yaml(file):
    if not os.path.exists(file):
        return {}
    with open(file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def update_yaml(file, new_data):
    if not os.path.exists(os.path.dirname(file)):
        os.makedirs(os.path.dirname(file))
    data = read_yaml(file)
    merge(data, new_data)
    with open(file, "w") as outfile:
        yaml.dump(data, outfile, default_flow_style=False)


def recursive_dict_update(defaults, updates):
    for key, value in updates.items():
        if isinstance(value, dict) and key in defaults:
            recursive_dict_update(defaults[key], value)
        else:
            defaults[key] = value
    return defaults


class Config:
    runtime_key = ''

    def __init__(self, **kwargs):
        # Set default directories
        self.config_path = os.path.join(get_home_dir(), '.tvh_iptv_config')
        self.config_file = os.path.join(self.config_path, 'settings.yml')
        # Set default settings
        self.settings = None
        self.tvh_local = is_tvh_process_running_locally_sync()
        self.default_settings = {
            "settings": {
                "first_run":                True,
                "tvheadend":                {
                    "host":     "",
                    "port":     "9981",
                    "path":     "/",
                    "username": "",
                    "password": "",
                },
                "app_url":                  None,
                "enable_admin_user":        True if self.tvh_local else False,
                "admin_password":           "admin",
                "enable_stream_buffer":     True,
                "default_ffmpeg_pipe_args": "-hide_banner -loglevel error "
                                            "-probesize 10M -analyzeduration 0 -fpsprobesize 0 "
                                            "-i [URL] -c copy -metadata service_name=[SERVICE_NAME] "
                                            "-f mpegts pipe:1",
                "create_client_user":       True,
                "client_username":          "client",
                "client_password":          "client",
                "epgs":                     {
                    "enable_tmdb_metadata":                False,
                    "tmdb_api_key":                        "",
                    "enable_google_image_search_metadata": False,
                }

            }
        }

    def create_default_settings_yaml(self):
        self.write_settings_yaml(self.default_settings)

    def write_settings_yaml(self, data):
        write_yaml(self.config_file, data)

    def read_config_yaml(self):
        if not os.path.exists(self.config_file):
            self.create_default_settings_yaml()
        return read_yaml(self.config_file)

    def read_settings(self):
        yaml_settings = {}
        if self.settings is None:
            yaml_settings = self.read_config_yaml()
        self.settings = recursive_dict_update(self.default_settings, yaml_settings)
        return self.settings

    def save_settings(self):
        if self.settings is None:
            self.create_default_settings_yaml()
        self.write_settings_yaml(self.settings)

    def update_settings(self, updated_settings):
        if self.settings is None:
            self.read_settings()
        self.settings = recursive_dict_update(self.default_settings, updated_settings)

    async def tvh_connection_settings(self):
        settings = await asyncio.to_thread(self.read_settings)
        if await is_tvh_process_running_locally():
            # Note: Host can be localhost here because the app will publish to TVH from the backend
            tvh_host = '127.0.0.1'
            tvh_port = '9981'
            tvh_path = '/tic-tvh'
            tvh_username = 'admin'
            tvh_password = await get_local_tvh_proc_admin_password()
            return {
                'tvh_local':    True,
                'tvh_host':     tvh_host,
                'tvh_port':     tvh_port,
                'tvh_path':     tvh_path,
                'tvh_username': tvh_username,
                'tvh_password': tvh_password,
            }
        return {
            'tvh_local':    False,
            'tvh_host':     settings['settings']['tvheadend']['host'],
            'tvh_port':     settings['settings']['tvheadend']['port'],
            'tvh_path':     settings['settings']['tvheadend']['path'],
            'tvh_username': settings['settings']['tvheadend']['username'],
            'tvh_password': settings['settings']['tvheadend']['password'],
        }


frontend_dir = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'frontend')

enable_app_debugging = False
if os.environ.get('ENABLE_APP_DEBUGGING', 'false').lower() == 'true':
    enable_app_debugging = True

enable_sqlalchemy_debugging = False
if os.environ.get('ENABLE_SQLALCHEMY_DEBUGGING', 'false').lower() == 'true':
    enable_sqlalchemy_debugging = True

flask_run_host = os.environ.get('FLASK_RUN_HOST', '0.0.0.0')
flask_run_port = int(os.environ.get('FLASK_RUN_PORT', '9985'))

app_basedir = os.path.abspath(os.path.dirname(__file__))
config_path = os.path.join(get_home_dir(), '.tvh_iptv_config')
if not os.path.exists(config_path):
    os.makedirs(config_path)

# Configure SQLite DB
sqlalchemy_database_path = os.path.join(config_path, 'db.sqlite3')
sqlalchemy_database_uri = 'sqlite:///' + sqlalchemy_database_path
sqlalchemy_database_async_uri = 'sqlite+aiosqlite:///' + sqlalchemy_database_path
sqlalchemy_track_modifications = False

# Configure scheduler
scheduler_api_enabled = True

# Set up the App SECRET_KEY
# SECRET_KEY = config('SECRET_KEY'  , default='S#perS3crEt_007')
secret_key = os.getenv('SECRET_KEY', 'S#perS3crEt_007')

# Assets Management
assets_root = os.getenv('ASSETS_ROOT', os.path.join(frontend_dir, 'dist', 'spa'))
