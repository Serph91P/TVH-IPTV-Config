#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import gzip
import json
import logging
import os
import asyncio
import time
import xml.etree.ElementTree as ET
from functools import lru_cache
from urllib.parse import quote

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from quart.utils import run_sync
from sqlalchemy.orm import joinedload
from sqlalchemy import and_, delete, insert, select, func

from backend.channels import read_base46_image_string
from backend.models import Session, Epg, Channel, EpgChannels, EpgChannelProgrammes
from backend.tvheadend.tvh_requests import get_tvh

logger = logging.getLogger('tic.epgs')

# Constants
XML_CHUNK_SIZE = 8192
REQUEST_TIMEOUT = 60  # seconds
DOWNLOAD_CHUNK_SIZE = 8192
MAX_CONCURRENT_REQUESTS = 10
CACHE_TTL = 600  # seconds


def generate_epg_channel_id(number, name):
    """Generate a standardized channel ID for EPG data."""
    return str(number)


async def read_config_all_epgs(output_for_export=False):
    """Read all EPG configurations."""
    return_list = []
    async with Session() as session:
        query = select(Epg)
        result = await session.execute(query)
        epgs = result.scalars().all()
        
        for epg in epgs:
            if output_for_export:
                return_list.append({
                    'enabled': epg.enabled,
                    'name': epg.name,
                    'url': epg.url,
                })
                continue
                
            return_list.append({
                'id': epg.id,
                'enabled': epg.enabled,
                'name': epg.name,
                'url': epg.url,
            })
            
    return return_list


async def read_config_one_epg(epg_id):
    """Read a specific EPG configuration by ID."""
    return_item = {}
    async with Session() as session:
        query = select(Epg).where(Epg.id == epg_id)
        result = await session.execute(query)
        epg = result.scalar_one_or_none()
        
        if epg:
            return_item = {
                'id': epg.id,
                'enabled': epg.enabled,
                'name': epg.name,
                'url': epg.url,
            }
            
    return return_item


async def add_new_epg(data):
    """Add a new EPG configuration."""
    async with Session() as session:
        async with session.begin():
            epg = Epg(
                enabled=data.get('enabled'),
                name=data.get('name'),
                url=data.get('url'),
            )
            session.add(epg)


async def update_epg(epg_id, data):
    """Update an existing EPG configuration."""
    async with Session() as session:
        async with session.begin():
            query = select(Epg).where(Epg.id == epg_id)
            result = await session.execute(query)
            epg = result.scalar_one()
            
            epg.enabled = data.get('enabled')
            epg.name = data.get('name')
            epg.url = data.get('url')


async def delete_epg(config, epg_id):
    """Delete an EPG configuration and all related data."""
    async with Session() as session:
        async with session.begin():
            # Get all channel IDs for the given EPG
            result = await session.execute(
                select(EpgChannels.id).where(EpgChannels.epg_id == epg_id)
            )
            channel_ids = [row[0] for row in result.fetchall()]
            
            if channel_ids:
                # Delete all related programmes
                await session.execute(
                    delete(EpgChannelProgrammes).where(
                        EpgChannelProgrammes.epg_channel_id.in_(channel_ids)
                    )
                )
                
                # Delete all related channels
                await session.execute(
                    delete(EpgChannels).where(EpgChannels.id.in_(channel_ids))
                )
                
            # Delete the EPG entry
            await session.execute(delete(Epg).where(Epg.id == epg_id))

    # Remove cached files
    cache_files = [
        os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml"),
        os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.yml"),
    ]
    
    for filepath in cache_files:
        if os.path.isfile(filepath):
            os.remove(filepath)


async def clear_epg_channel_data(epg_id):
    """Clear all channel data for an EPG without deleting the EPG itself."""
    async with Session() as session:
        async with session.begin():
            # Get all channel IDs for the given EPG
            result = await session.execute(
                select(EpgChannels.id).where(EpgChannels.epg_id == epg_id)
            )
            channel_ids = [row[0] for row in result.fetchall()]
            
            if channel_ids:
                # Delete all related programmes
                await session.execute(
                    delete(EpgChannelProgrammes).where(
                        EpgChannelProgrammes.epg_channel_id.in_(channel_ids)
                    )
                )
                
                # Delete all related channels
                await session.execute(
                    delete(EpgChannels).where(EpgChannels.id.in_(channel_ids))
                )


async def download_xmltv_epg(url, output):
    """Download an XMLTV EPG file from a URL."""
    logger.info("Downloading EPG from url - '%s'", url)
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output), exist_ok=True)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
    }
    
    # Use a timeout to prevent hanging downloads
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                
                # Stream the response to file
                async with aiofiles.open(output, 'wb') as f:
                    async for chunk in response.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
                        await f.write(chunk)
                        
        # Try to unzip the file if it's compressed
        await try_unzip(output)
        return True
        
    except aiohttp.ClientError as e:
        logger.error("Error downloading EPG: %s", str(e))
        return False
    except Exception as e:
        logger.error("Unexpected error downloading EPG: %s", str(e))
        return False


async def try_unzip(output: str) -> None:
    """Attempt to unzip a file if it's gzipped."""
    loop = asyncio.get_event_loop()
    
    try:
        # Use run_in_executor for CPU-bound gzip operation
        def _unzip():
            with gzip.open(output, 'rb') as f:
                content = f.read()
            with open(output, 'wb') as f:
                f.write(content)
                
        await loop.run_in_executor(None, _unzip)
        logger.info("Downloaded file was gzipped. Successfully unzipped.")
        
    except Exception:
        # Not a gzip file or other error, ignore
        pass


async def store_epg_channels(config, epg_id):
    """Parse and store EPG channel information from an XMLTV file."""
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    
    if not os.path.exists(xmltv_file):
        logger.error("No such file '%s'", xmltv_file)
        return False
    
    # Read XML file contents
    try:
        async with aiofiles.open(xmltv_file, mode='r', encoding="utf8", errors='ignore') as f:
            contents = await f.read()
            
        # Parse XML
        input_file = ET.ElementTree(ET.fromstring(contents))
        
        # Process channels in database transaction
        async with Session() as session:
            async with session.begin():
                # Delete existing EPG channels
                await session.execute(
                    delete(EpgChannels).where(EpgChannels.epg_id == epg_id)
                )
                
                # Extract channel information
                logger.info("Updating channels list for EPG #%s from path - '%s'", epg_id, xmltv_file)
                items = []
                channel_id_list = []
                
                # Process all channels
                for channel in input_file.iterfind('.//channel'):
                    channel_id = channel.get('id')
                    display_name_elem = channel.find('display-name')
                    
                    if display_name_elem is None:
                        logger.warning("Channel without display-name: %s", channel_id)
                        continue
                        
                    display_name = display_name_elem.text
                    
                    # Extract icon URL if available
                    icon = ''
                    icon_elem = channel.find('icon')
                    if icon_elem is not None:
                        icon = icon_elem.attrib.get('src', '')
                    
                    logger.debug("Channel ID: '%s', Display Name: '%s', Icon: %s", 
                                channel_id, display_name, icon)
                    
                    items.append({
                        'epg_id': epg_id,
                        'channel_id': channel_id,
                        'name': display_name,
                        'icon_url': icon,
                    })
                    channel_id_list.append(channel_id)
                
                # Bulk insert all channels
                if items:
                    await session.execute(insert(EpgChannels), items)
                
                logger.info("Successfully imported %s channels from path - '%s'", 
                           len(channel_id_list), xmltv_file)
                
        return channel_id_list
        
    except ET.ParseError as e:
        logger.error("XML parsing error in file %s: %s", xmltv_file, str(e))
        return False
    except Exception as e:
        logger.error("Error processing EPG channels from %s: %s", xmltv_file, str(e))
        return False


async def store_epg_programmes(config, epg_id, channel_id_list):
    """Parse and store EPG programme information from an XMLTV file."""
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    
    if not os.path.exists(xmltv_file):
        logger.error("No such file '%s'", xmltv_file)
        return False

    def parse_and_save_programmes():
        """Parse XML file and save programmes to database."""
        try:
            # Parse XML file
            input_file = ET.parse(xmltv_file)
            
            # Build a mapping of channel IDs to EPG channel IDs
            logger.info("Fetching list of channels from EPG #%s from database", epg_id)
            from backend.models import db
            
            channel_ids = {}
            for channel_id in channel_id_list:
                epg_channel = db.session.query(EpgChannels) \
                    .filter(and_(EpgChannels.channel_id == channel_id,
                                EpgChannels.epg_id == epg_id
                                )) \
                    .first()
                if epg_channel:
                    channel_ids[channel_id] = epg_channel.id
            
            # Process all programmes
            logger.info("Updating programmes list for EPG #%s from path - '%s'", epg_id, xmltv_file)
            items = []
            count = 0
            
            # Process programmes in batches for better memory efficiency
            batch_size = 1000
            for programme in input_file.iterfind(".//programme"):
                if len(items) >= batch_size:
                    db.session.bulk_save_objects(items, update_changed_only=False)
                    db.session.commit()
                    count += len(items)
                    items = []
                
                channel_id = programme.attrib.get('channel', None)
                if channel_id in channel_ids:
                    epg_channel_id = channel_ids.get(channel_id)
                    
                    # Parse attributes
                    start = programme.attrib.get('start', None)
                    stop = programme.attrib.get('stop', None)
                    start_timestamp = programme.attrib.get('start_timestamp', None)
                    stop_timestamp = programme.attrib.get('stop_timestamp', None)
                    
                    # Parse elements
                    title = programme.findtext("title", default=None)
                    sub_title = programme.findtext("sub-title", default=None)
                    desc = programme.findtext("desc", default=None)
                    series_desc = programme.findtext("series-desc", default=None)
                    country = programme.findtext("country", default=None)
                    
                    # Parse icon
                    icon = programme.find("icon")
                    icon_url = icon.attrib.get('src', None) if icon is not None else None
                    
                    # Parse categories
                    categories = []
                    for category in programme.findall("category"):
                        if category.text:
                            categories.append(category.text)
                    
                    # Create programme record
                    items.append(
                        EpgChannelProgrammes(
                            epg_channel_id=epg_channel_id,
                            channel_id=channel_id,
                            title=title,
                            sub_title=sub_title,
                            desc=desc,
                            series_desc=series_desc,
                            icon_url=icon_url,
                            country=country,
                            start=start,
                            stop=stop,
                            start_timestamp=start_timestamp,
                            stop_timestamp=stop_timestamp,
                            categories=json.dumps(categories)
                        )
                    )
            
            # Save any remaining items
            if items:
                db.session.bulk_save_objects(items, update_changed_only=False)
                db.session.commit()
                count += len(items)
            
            logger.info("Successfully imported %s programmes from path - '%s'", count, xmltv_file)
            return True
            
        except ET.ParseError as e:
            logger.error("XML parsing error in file %s: %s", xmltv_file, str(e))
            return False
        except Exception as e:
            logger.error("Error processing EPG programmes from %s: %s", xmltv_file, str(e))
            return False

    # Run the parsing function synchronously since it's CPU-bound
    return await run_sync(parse_and_save_programmes)()


async def load_epg_channel_options(epg_id):
    """Load channel options for a specific EPG."""
    async with Session() as session:
        query = select(EpgChannels).where(EpgChannels.epg_id == epg_id)
        result = await session.execute(query)
        channels = result.scalars().all()
        
        return_list = []
        for channel in channels:
            return_list.append({
                'id': channel.id,
                'channel_id': channel.channel_id,
                'name': channel.name,
                'icon_url': channel.icon_url,
            })
            
        return return_list


@lru_cache(maxsize=128)
def get_program_short_description(description, max_length=100):
    """Extract a short description from a full program description, with caching."""
    if not description:
        return ""
        
    # Extract first sentence or limit by max_length
    sentences = description.split('.')
    if sentences:
        short_desc = sentences[0].strip()
        if len(short_desc) > max_length:
            return short_desc[:max_length] + "..."
        return short_desc
    return ""


async def build_custom_epg(config):
    """Build a custom EPG XML file from the database."""
    logger.info("Creating static XMLTV EPG file")

    # Ensure cache directory exists
    output_dir = os.path.join(config.config_path)
    epg_file = os.path.join(output_dir, "epg.xml")
    os.makedirs(output_dir, exist_ok=True)

    # Open file for writing
    async with aiofiles.open(epg_file, 'w', encoding='utf-8') as f:
        # Write XML header
        await f.write('<?xml version="1.0" encoding="utf-8" ?>\n')
        await f.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        await f.write('<tv generator-info-name="TVH-IPTV-Config" generator-info-url="https://github.com/josh5/TVH-IPTV-Config">\n')

        # Fetch all enabled channels with their sources and guide information
        async with Session() as session:
            # Query channels with eager loading of related data
            query = select(Channel).options(
                joinedload(Channel.guide)
            ).filter(Channel.enabled == True)
            
            result = await session.execute(query)
            channels = result.unique().scalars().all()

            # Process all channels
            for channel in channels:
                # Generate channel ID
                channel_id = generate_epg_channel_id(channel.number, channel.name)
                
                # Write channel element
                await f.write(f'  <channel id="{channel_id}">\n')
                await f.write(f'    <display-name>{channel.name}</display-name>\n')
                
                if channel.logo_url:
                    await f.write(f'    <icon src="{channel.logo_url}"/>\n')
                    
                await f.write('  </channel>\n')

            # Process programmes for all channels
            for channel in channels:
                if not channel.guide_id or not channel.guide_channel_id:
                    continue
                    
                channel_id = generate_epg_channel_id(channel.number, channel.name)
                
                # Query programmes for this channel
                programmes_query = select(EpgChannelProgrammes).join(
                    EpgChannels, 
                    EpgChannelProgrammes.epg_channel_id == EpgChannels.id
                ).filter(
                    EpgChannels.epg_id == channel.guide_id,
                    EpgChannelProgrammes.channel_id == channel.guide_channel_id
                )
                
                programmes_result = await session.execute(programmes_query)
                programmes = programmes_result.scalars().all()
                
                # Write programme elements
                for programme in programmes:
                    await f.write(f'  <programme start="{programme.start}" stop="{programme.stop}" channel="{channel_id}">\n')
                    
                    if programme.title:
                        await f.write(f'    <title>{programme.title}</title>\n')
                        
                    if programme.sub_title:
                        await f.write(f'    <sub-title>{programme.sub_title}</sub-title>\n')
                        
                    if programme.desc:
                        await f.write(f'    <desc>{programme.desc}</desc>\n')
                        
                    if programme.icon_url:
                        await f.write(f'    <icon src="{programme.icon_url}"/>\n')
                        
                    if programme.categories:
                        try:
                            categories = json.loads(programme.categories)
                            for category in categories:
                                await f.write(f'    <category>{category}</category>\n')
                        except (json.JSONDecodeError, TypeError):
                            # Handle invalid JSON
                            pass
                            
                    await f.write('  </programme>\n')

        # Close the TV tag
        await f.write('</tv>\n')

    logger.info("Completed static XMLTV EPG file - %s", epg_file)
    return epg_file


async def run_tvh_epg_grabbers(config):
    """Trigger TVHeadend EPG grabbers to update programme information."""
    logger.info("Triggering an update in TVH to fetch the latest EPG")
    
    try:
        async with await get_tvh(config) as tvh:
            await tvh.trigger_epg_grabbers()
            return True
    except Exception as e:
        logger.error("Failed to trigger EPG grabbers: %s", str(e))
        return False


async def fetch_tmdb_movie_info(title, api_key):
    """Fetch movie information from TMDB API."""
    if not api_key:
        return None
        
    encoded_title = quote(title)
    url = f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={encoded_title}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('results') and len(data['results']) > 0:
                        movie = data['results'][0]
                        poster_path = movie.get('poster_path')
                        if poster_path:
                            poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
                            return {
                                'title': movie.get('title'),
                                'overview': movie.get('overview'),
                                'poster_url': poster_url,
                                'release_date': movie.get('release_date'),
                            }
        return None
    except Exception as e:
        logger.error("Error fetching TMDB data: %s", str(e))
        return None


async def google_image_search(query):
    """Search for images using Google's image search."""
    # Add movie/TV show keywords to improve results
    search_query = f"{query} movie tv show poster official"
    encoded_query = quote(search_query)
    url = f"https://www.google.com/search?q={encoded_query}&tbm=isch"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Look for image results
                    img_tags = soup.find_all('img')
                    img_urls = []
                    
                    for img in img_tags[1:10]:  # Skip first (usually Google logo) and limit results
                        src = img.get('src')
                        if src and src.startswith('http') and 'gstatic' not in src:
                            img_urls.append(src)
                            
                    return img_urls[0] if img_urls else None
        return None
    except Exception as e:
        logger.error("Error performing Google image search: %s", str(e))
        return None


async def update_programme_with_metadata(programme, settings):
    """Update a programme with metadata from external sources."""
    try:
        # Check if we should update TMDB metadata
        if settings.get('enable_tmdb_metadata') and programme.title:
            api_key = settings.get('tmdb_api_key')
            movie_info = await fetch_tmdb_movie_info(programme.title, api_key)
            
            if movie_info:
                if not programme.desc and movie_info.get('overview'):
                    programme.desc = movie_info['overview']
                if not programme.icon_url and movie_info.get('poster_url'):
                    programme.icon_url = movie_info['poster_url']
                    
        # Check if we should update with Google image search
        if settings.get('enable_google_image_search_metadata') and programme.title and not programme.icon_url:
            img_url = await google_image_search(programme.title)
            if img_url:
                programme.icon_url = img_url
                
        return programme
    except Exception as e:
        logger.error("Error updating programme: %s", str(e))
        return programme


async def update_channel_epg_with_online_data(config):
    """Update EPG data with online metadata for all channels."""
    logger.info("Update EPG with missing data from online sources for each configured channel.")
    
    settings = config.read_settings()
    epg_settings = settings['settings'].get('epgs', {})
    
    # Skip if no metadata sources are enabled
    if not (epg_settings.get('enable_tmdb_metadata') or epg_settings.get('enable_google_image_search_metadata')):
        logger.info("No metadata sources enabled. Skipping EPG update.")
        return
        
    # Get all channels with EPG data
    async with Session() as session:
        channels_query = select(Channel).filter(
            Channel.guide_id.isnot(None),
            Channel.guide_channel_id.isnot(None)
        )
        
        channels_result = await session.execute(channels_query)
        channels = channels_result.scalars().all()
        
        # Process each channel's programmes
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        for channel in channels:
            logger.info("   - Updating programme list for %s - %s.", channel.number, channel.name)
            
            # Get programmes for this channel
            programmes_query = select(EpgChannelProgrammes).join(
                EpgChannels,
                EpgChannelProgrammes.epg_channel_id == EpgChannels.id
            ).filter(
                EpgChannels.epg_id == channel.guide_id,
                EpgChannelProgrammes.channel_id == channel.guide_channel_id
            )
            
            programmes_result = await session.execute(programmes_query)
            programmes = programmes_result.scalars().all()
            
            # Process programmes with concurrency limit
            update_tasks = []
            
            async def process_programme(prog):
                async with semaphore:
                    try:
                        updated_prog = await update_programme_with_metadata(prog, epg_settings)
                        return updated_prog
                    except Exception as e:
                        logger.error("Error updating programme: %s", str(e))
                        return prog
            
            for programme in programmes:
                update_tasks.append(process_programme(programme))
                
            # Wait for all updates to complete
            updated_programmes = await asyncio.gather(*update_tasks, return_exceptions=True)
            
            # Filter out exceptions and save updated programmes
            valid_programmes = [p for p in updated_programmes if not isinstance(p, Exception)]
            
            for programme in valid_programmes:
                session.add(programme)
                
            await session.commit()


async def delete_epg_programme_data(config, epg_id, epg_channel_id, programme_id=None):
    """Delete programme data for an EPG channel."""
    async with Session() as session:
        async with session.begin():
            if programme_id:
                # Delete a single programme
                await session.execute(
                    delete(EpgChannelProgrammes).where(
                        EpgChannelProgrammes.id == programme_id
                    )
                )
            else:
                # Delete all programmes for the channel
                await session.execute(
                    delete(EpgChannelProgrammes).where(
                        EpgChannelProgrammes.epg_channel_id == epg_channel_id
                    )
                )

