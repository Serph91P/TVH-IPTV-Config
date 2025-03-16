import asyncio
import base64
import json
import os
import subprocess
from functools import lru_cache

import aiofiles
import yaml
from mergedeep import merge


def get_home_dir():
    """Get user home directory with environment variable fallback."""
    return os.environ.get('HOME_DIR') or os.path.expanduser("~")


async def is_tvh_process_running_locally():
    """Check if TVHeadend is running locally using async subprocess."""
    process_name = 'tvheadend'
    try:
        process = await asyncio.create_subprocess_exec(
            'pgrep', '-x', process_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        return process.returncode == 0
    except Exception as e:
        print(f"Error checking for TVHeadend process: {e}")
        return False


# Cache the result for 60 seconds to avoid frequent process checks
@lru_cache(maxsize=1)
def is_tvh_process_running_locally_sync():
    """Cached sync version of TVHeadend process check."""
    process_name = 'tvheadend'
    try:
        result = subprocess.run(
            ['pgrep', '-x', process_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=2  # Add timeout to prevent hanging
        )
        return result.returncode == 0
    except Exception as e:
        print(f"Error checking for TVHeadend process: {e}")
        return False


async def get_admin_file(directory):
    """Find and parse the admin configuration file."""
    if not os.path.exists(directory) or not os.listdir(directory):
        return None, None
        
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if not os.path.isfile(file_path):
            continue
            
        try:
            async with aiofiles.open(file_path, 'r') as file:
                contents = await file.read()
                data = json.loads(contents)
                if data.get('username') == 'admin':
                    return file_path, data
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error processing file {file_path}: {e}")
            
    return None, None


async def update_accesscontrol_files():
    """Update TVHeadend access control files to allow remote connections."""
    accesscontrol_path = os.path.join(get_home_dir(), '.tvheadend', 'accesscontrol')
    file_path, data = await get_admin_file(accesscontrol_path)
    
    if not data:
        return False
        
    data['prefix'] = '0.0.0.0/0,::/0'
    
    try:
        async with aiofiles.open(file_path, 'w') as outfile:
            await outfile.write(json.dumps(data, indent=4))
        return True
    except Exception as e:
        print(f"Error updating access control file: {e}")
        return False


async def get_local_tvh_proc_admin_password():
    """Extract admin password from TVHeadend password file."""
    passwd_path = os.path.join(get_home_dir(), '.tvheadend', 'passwd')
    file_path, data = await get_admin_file(passwd_path)
    
    if not data:
        return None
        
    try:
        encoded_password = data.get('password2')
        if not encoded_password:
            return None
            
        decoded_password = base64.b64decode(encoded_password).decode('utf-8')
        parts = decoded_password.split('-')
        if len(parts) >= 3:
            return parts[2]
    except Exception as e:
        print(f"Error decoding password: {e}")
        
    return None


# File I/O operations with better error handling
def write_yaml(file, data):
    """Write data to YAML file with directory creation if needed."""
    directory = os.path.dirname(file)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        
    try:
        with open(file, "w") as outfile:
            yaml.dump(data, outfile, default_flow_style=False)
        return True
    except Exception as e:
        print(f"Error writing YAML file {file}: {e}")
        return False


def read_yaml(file):
    """Read and parse YAML file with error handling."""
    if not os.path.exists(file):
        return {}
        
    try:
        with open(file, "r") as stream:
            return yaml.safe_load(stream) or {}
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file {file}: {exc}")
        return {}
    except Exception as e:
        print(f"Error reading YAML file {file}: {e}")
        return {}


def update_yaml(file, new_data):
    """Update existing YAML file with new data."""
    directory = os.path.dirname(file)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        
    try:
        data = read_yaml(file)
        merge(data, new_data)
        with open(file, "w") as outfile:
            yaml.dump(data, outfile, default_flow_style=False)
        return True
    except Exception as e:
        print(f"Error updating YAML file {file}: {e}")
        return False


def recursive_dict_update(defaults, updates):
    """Recursively update a dictionary with another dictionary."""
    if not isinstance(updates, dict):
        return defaults
        
    for key, value in updates.items():
        if isinstance(value, dict) and key in defaults and isinstance(defaults[key], dict):
            recursive_dict_update(defaults[key], value)
        else:
            defaults[key] = value
            
    return defaults


class Config:
    """Configuration manager for TVH-IPTV-Config."""
    
    def __init__(self, **kwargs):
        # Set default directories
        self.config_path = os.path.join(get_home_dir(), '.tvh_iptv_config')
        self.config_file = os.path.join(self.config_path, 'settings.yml')
        
        # Cached settings
        self._settings_cache = None
        self._settings_cache_time = 0
        self._cache_ttl = 10  # Cache TTL in seconds
        
        # Check for TVH locally
        self.tvh_local = is_tvh_process_running_locally_sync()
        
        # Default settings
        self.default_settings = {
            "settings": {
                "first_run": True,
                "tvheadend": {
                    "host": "",
                    "port": "9981",
                    "path": "/",
                    "username": "",
                    "password": "",
                },
                "app_url": None,
                "enable_admin_user": True if self.tvh_local else False,
                "admin_password": "admin",
                "enable_stream_buffer": True,
                "default_ffmpeg_pipe_args": "-hide_banner -loglevel error "
                                           "-probesize 10M -analyzeduration 0 -fpsprobesize 0 "
                                           "-i [URL] -c copy -metadata service_name=[SERVICE_NAME] "
                                           "-f mpegts pipe:1",
                "create_client_user": True,
                "client_username": "client",
                "client_password": "client",
                "epgs": {
                    "enable_tmdb_metadata": False,
                    "tmdb_api_key": "",
                    "enable_google_image_search_metadata": False,
                }
            }
        }

    def create_default_settings_yaml(self):
        """Create default settings YAML file."""
        self.write_settings_yaml(self.default_settings)
        # Clear cache after writing
        self._settings_cache = None
        
    def write_settings_yaml(self, data):
        """Write settings to YAML file."""
        result = write_yaml(self.config_file, data)
        # Clear cache after writing
        self._settings_cache = None
        return result
        
    def read_config_yaml(self):
        """Read configuration from YAML file."""
        if not os.path.exists(self.config_file):
            self.create_default_settings_yaml()
        return read_yaml(self.config_file)
        
    def read_settings(self):
        """Read settings with caching for performance."""
        # Use cached settings if available and not expired
        current_time = asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else time.time()
        if (self._settings_cache is not None and 
            current_time - self._settings_cache_time < self._cache_ttl):
            return self._settings_cache
            
        # Read settings from file
        yaml_settings = self.read_config_yaml()
        self._settings_cache = recursive_dict_update(
            self.default_settings.copy(), yaml_settings)
        self._settings_cache_time = current_time
        
        return self._settings_cache
        
    def save_settings(self):
        """Save current settings to YAML file."""
        if self._settings_cache is None:
            self.create_default_settings_yaml()
        else:
            self.write_settings_yaml(self._settings_cache)
            
    def update_settings(self, updated_settings):
        """Update settings with new values."""
        if self._settings_cache is None:
            self.read_settings()
            
        self._settings_cache = recursive_dict_update(
            self.default_settings.copy(), updated_settings)
        
    async def tvh_connection_settings(self):
        """Get TVHeadend connection settings."""
        # Use cached settings for sync operations
        settings = self.read_settings()
        
        # Check if TVH is running locally
        if await is_tvh_process_running_locally():
            # Get admin password
            tvh_password = await get_local_tvh_proc_admin_password()
            return {
                'tvh_local': True,
                'tvh_host': '127.0.0.1',
                'tvh_port': '9981',
                'tvh_path': '/tic-tvh',
                'tvh_username': 'admin',
                'tvh_password': tvh_password,
            }
            
        # Return configured remote settings
        return {
            'tvh_local': False,
            'tvh_host': settings['settings']['tvheadend']['host'],
            'tvh_port': settings['settings']['tvheadend']['port'],
            'tvh_path': settings['settings']['tvheadend']['path'],
            'tvh_username': settings['settings']['tvheadend']['username'],
            'tvh_password': settings['settings']['tvheadend']['password'],
        }


# Add missing import
import time

# Global configuration
frontend_dir = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'frontend')

# Environment configuration with defaults
enable_app_debugging = os.environ.get('ENABLE_APP_DEBUGGING', 'false').lower() == 'true'
enable_sqlalchemy_debugging = os.environ.get('ENABLE_SQLALCHEMY_DEBUGGING', 'false').lower() == 'true'
flask_run_host = os.environ.get('FLASK_RUN_HOST', '0.0.0.0')
flask_run_port = int(os.environ.get('FLASK_RUN_PORT', '9985'))

# Path configuration
app_basedir = os.path.abspath(os.path.dirname(__file__))
config_path = os.path.join(get_home_dir(), '.tvh_iptv_config')
if not os.path.exists(config_path):
    os.makedirs(config_path, exist_ok=True)

# Database configuration
sqlalchemy_database_path = os.path.join(config_path, 'db.sqlite3')
sqlalchemy_database_uri = 'sqlite:///' + sqlalchemy_database_path
sqlalchemy_database_async_uri = 'sqlite+aiosqlite:///' + sqlalchemy_database_path
sqlalchemy_track_modifications = False

# Scheduler configuration
scheduler_api_enabled = True

# Security configuration
secret_key = os.getenv('SECRET_KEY', 'S#perS3crEt_007')

# Assets configuration
assets_root = os.getenv('ASSETS_ROOT', os.path.join(frontend_dir, 'dist', 'spa'))
