#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import gzip
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from mimetypes import guess_extension
from urllib.parse import quote

import aiofiles
import aiohttp
import asyncio
import time
import xml.etree.ElementTree as ET
from types import NoneType

from bs4 import BeautifulSoup
from quart.utils import run_sync
from sqlalchemy.orm import joinedload
from sqlalchemy import and_, delete, insert, select
from backend.channels import read_base46_image_string
from backend.models import db, Session, Epg, Channel, EpgChannels, EpgChannelProgrammes
from backend.tvheadend.tvh_requests import get_tvh

logger = logging.getLogger('tic.epgs')


def generate_epg_channel_id(number, name):
    # return f"{number}_{re.sub(r'[^a-zA-Z0-9]', '', name)}"
    return str(number)


async def read_config_all_epgs(output_for_export=False):
    return_list = []
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Epg))
            results = query.scalars().all()
            for result in results:
                if output_for_export:
                    return_list.append({
                        'enabled': result.enabled,
                        'name':    result.name,
                        'url':     result.url,
                    })
                    continue
                return_list.append({
                    'id':      result.id,
                    'enabled': result.enabled,
                    'name':    result.name,
                    'url':     result.url,
                })
    return return_list


async def read_config_one_epg(epg_id):
    return_item = {}
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Epg).where(Epg.id == epg_id))
            results = query.scalar_one_or_none()
            if results:
                return_item = {
                    'id':      results.id,
                    'enabled': results.enabled,
                    'name':    results.name,
                    'url':     results.url,
                }
    return return_item


async def add_new_epg(data):
    async with Session() as session:
        async with session.begin():
            epg = Epg(
                enabled=data.get('enabled'),
                name=data.get('name'),
                url=data.get('url'),
            )
            # This is a new entry. Add it to the session before commit
            session.add(epg)


async def update_epg(epg_id, data):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(Epg).where(Epg.id == epg_id))
            epg = result.scalar_one()
            epg.enabled = data.get('enabled')
            epg.name = data.get('name')
            epg.url = data.get('url')


async def delete_epg(config, epg_id):
    async with Session() as session:
        async with session.begin():
            # Get all channel IDs for the given EPG
            # noinspection DuplicatedCode
            result = await session.execute(select(EpgChannels.id).where(EpgChannels.epg_id == epg_id))
            channel_ids = [row[0] for row in result.fetchall()]
            if channel_ids:
                # Delete all EpgChannelProgrammes where epg_channel_id is in the list of channel IDs
                await session.execute(
                    delete(EpgChannelProgrammes).where(EpgChannelProgrammes.epg_channel_id.in_(channel_ids)))
                # Delete all EpgChannels where id is in the list of channel IDs
                await session.execute(delete(EpgChannels).where(EpgChannels.id.in_(channel_ids)))
            # Delete the Epg entry
            await session.execute(delete(Epg).where(Epg.id == epg_id))

    # Remove cached copy of epg
    cache_files = [
        os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml"),
        os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.yml"),
    ]
    for f in cache_files:
        if os.path.isfile(f):
            os.remove(f)


async def clear_epg_channel_data(epg_id):
    async with Session() as session:
        async with session.begin():
            # Get all channel IDs for the given EPG
            # noinspection DuplicatedCode
            result = await session.execute(select(EpgChannels.id).where(EpgChannels.epg_id == epg_id))
            channel_ids = [row[0] for row in result.fetchall()]
            if channel_ids:
                # Delete all EpgChannelProgrammes where epg_channel_id is in the list of channel IDs
                await session.execute(
                    delete(EpgChannelProgrammes).where(EpgChannelProgrammes.epg_channel_id.in_(channel_ids)))
                # Delete all EpgChannels where id is in the list of channel IDs
                await session.execute(delete(EpgChannels).where(EpgChannels.id.in_(channel_ids)))


async def download_xmltv_epg(url, output):
    logger.info("Downloading EPG from url - '%s'", url)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            async with aiofiles.open(output, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
    await try_unzip(output)


async def try_unzip(output: str) -> None:
    loop = asyncio.get_event_loop()
    try:
        with gzip.open(output, 'rb') as f:
            out = f.readlines()
        logger.info("Downloaded file is gzipped. Unzipping")
        await loop.run_in_executor(None, lambda: open(output, 'wb').writelines(out))
    except:
        pass


async def store_epg_channels(config, epg_id):
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    if not os.path.exists(xmltv_file):
        logger.info("No such file '%s'", xmltv_file)
        return False
    # Read and parse XML file contents asynchronously
    async with aiofiles.open(xmltv_file, mode='r', encoding="utf8", errors='ignore') as f:
        contents = await f.read()
    input_file = ET.ElementTree(ET.fromstring(contents))
    async with Session() as session:
        async with session.begin():
            # Delete all existing EPG channels
            stmt = delete(EpgChannels).where(EpgChannels.epg_id == epg_id)
            await session.execute(stmt)
            # Add an updated list of channels from the XML file to the DB
            logger.info("Updating channels list for EPG #%s from path - '%s'", epg_id, xmltv_file)
            items = []
            channel_id_list = []
            for channel in input_file.iterfind('.//channel'):
                channel_id = channel.get('id')
                display_name = channel.find('display-name').text
                icon = ''
                icon_elem = channel.find('icon')
                if icon_elem is not None:
                    icon = icon_elem.attrib.get('src', '')
                logger.debug("Channel ID: '%s', Display Name: '%s', Icon: %s", channel_id, display_name, icon)
                items.append({
                    'epg_id':     epg_id,
                    'channel_id': channel_id,
                    'name':       display_name,
                    'icon_url':   icon,
                })
                channel_id_list.append(channel_id)
            # Perform bulk insert
            await session.execute(insert(EpgChannels), items)
            # Commit all updates to channels
            await session.commit()
            logger.info("Successfully imported %s channels from path - '%s'", len(channel_id_list), xmltv_file)
        # Return list of channels
        return channel_id_list


async def store_epg_programmes(config, epg_id, channel_id_list):
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    if not os.path.exists(xmltv_file):
        # TODO: Add error logging here
        logger.info("No such file '%s'", xmltv_file)
        return False

    def parse_and_save_programmes():
        # Open file
        input_file = ET.parse(xmltv_file)

        # For each channel, create a list of programmes
        logger.info("Fetching list of channels from EPG #%s from database", epg_id)
        channel_ids = {}
        for channel_id in channel_id_list:
            epg_channel = db.session.query(EpgChannels) \
                .filter(and_(EpgChannels.channel_id == channel_id,
                             EpgChannels.epg_id == epg_id
                             )) \
                .first()
            channel_ids[channel_id] = epg_channel.id
        # Add an updated list of programmes from the XML file to the DB
        logger.info("Updating new programmes list for EPG #%s from path - '%s'", epg_id, xmltv_file)
        items = []
        for programme in input_file.iterfind(".//programme"):
            if len(items) > 1000:
                db.session.bulk_save_objects(items, update_changed_only=False)
                items = []
            channel_id = programme.attrib.get('channel', None)
            if channel_id in channel_ids:
                epg_channel_id = channel_ids.get(channel_id)
                # Parse attributes first
                start = programme.attrib.get('start', None)
                stop = programme.attrib.get('stop', None)
                start_timestamp = programme.attrib.get('start_timestamp', None)
                stop_timestamp = programme.attrib.get('stop_timestamp', None)
                
                # Parse basic sub-elements
                title = programme.findtext("title", default=None)
                sub_title = programme.findtext("sub-title", default=None)
                desc = programme.findtext("desc", default=None)
                series_desc = programme.findtext("series-desc", default=None)
                country = programme.findtext("country", default=None)
                url = programme.findtext("url", default=None)
                date = programme.findtext("date", default=None)
                
                # Parse icon
                icon = programme.find("icon")
                icon_url = icon.attrib.get('src', None) if icon is not None else None
                
                # Parse categories
                categories = []
                for category in programme.findall("category"):
                    if category.text:
                        categories.append(category.text)
                
                # Parse keywords
                keywords = []
                for keyword in programme.findall("keyword"):
                    if keyword.text:
                        keywords.append(keyword.text)
                
                # Parse credits
                credits_data = {}
                credits_elem = programme.find("credits")
                if credits_elem is not None:
                    for credit_type in ['actor', 'director', 'writer', 'producer', 'presenter', 'commentator', 'guest']:
                        credit_list = []
                        for credit in credits_elem.findall(credit_type):
                            if credit.text:
                                credit_list.append(credit.text)
                        if credit_list:
                            credits_data[credit_type] = credit_list
                
                # Parse episode number
                episode_num_system = None
                episode_num_value = None
                episode_num = programme.find("episode-num")
                if episode_num is not None:
                    episode_num_system = episode_num.attrib.get('system', None)
                    episode_num_value = episode_num.text
                
                # Parse rating
                rating_system = None
                rating_value = None
                rating = programme.find("rating")
                if rating is not None:
                    rating_system = rating.attrib.get('system', None)
                    rating_value_elem = rating.find("value")
                    if rating_value_elem is not None:
                        rating_value = rating_value_elem.text
                
                # Parse star rating
                star_rating = None
                star_rating_elem = programme.find("star-rating")
                if star_rating_elem is not None:
                    star_rating_value = star_rating_elem.find("value")
                    if star_rating_value is not None:
                        star_rating = star_rating_value.text
                
                # Parse video information
                video_present = None
                video_colour = None
                video_aspect = None
                video_quality = None
                video = programme.find("video")
                if video is not None:
                    video_present_elem = video.find("present")
                    if video_present_elem is not None:
                        video_present = video_present_elem.text.lower() == 'yes'
                    
                    video_colour_elem = video.find("colour")
                    if video_colour_elem is not None:
                        video_colour = video_colour_elem.text.lower() == 'yes'
                    
                    video_aspect_elem = video.find("aspect")
                    if video_aspect_elem is not None:
                        video_aspect = video_aspect_elem.text
                    
                    video_quality_elem = video.find("quality")
                    if video_quality_elem is not None:
                        video_quality = video_quality_elem.text
                
                # Parse audio information
                audio_present = None
                audio_stereo = None
                audio = programme.find("audio")
                if audio is not None:
                    audio_present_elem = audio.find("present")
                    if audio_present_elem is not None:
                        audio_present = audio_present_elem.text.lower() == 'yes'
                    
                    audio_stereo_elem = audio.find("stereo")
                    if audio_stereo_elem is not None:
                        audio_stereo = audio_stereo_elem.text
                
                # Parse accessibility features
                subtitles_type = None
                subtitles = programme.find("subtitles")
                if subtitles is not None:
                    subtitles_type = subtitles.attrib.get('type', None)
                
                audio_described = None
                if programme.find("audio-described") is not None:
                    audio_described = True
                
                # Parse programme flags
                is_premiere = programme.find("premiere") is not None
                is_new = programme.find("new") is not None
                
                # Parse previously shown
                previously_shown = None
                previously_shown_elem = programme.find("previously-shown")
                if previously_shown_elem is not None:
                    previously_shown = previously_shown_elem.attrib.get('start', None)
                
                # Parse length
                length = None
                length_elem = programme.find("length")
                if length_elem is not None:
                    length_units = length_elem.attrib.get('units', 'minutes')
                    length = f"{length_elem.text} {length_units}" if length_elem.text else None
                
                # Parse review
                review_type = None
                review_value = None
                review = programme.find("review")
                if review is not None:
                    review_type = review.attrib.get('type', None)
                    review_value = review.text
                
                # Create new line entry for the programmes table
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
                        categories=json.dumps(categories) if categories else None,
                        url=url,
                        date=date,
                        length=length,
                        keywords=json.dumps(keywords) if keywords else None,
                        credits=json.dumps(credits_data) if credits_data else None,
                        episode_num_system=episode_num_system,
                        episode_num_value=episode_num_value,
                        rating_system=rating_system,
                        rating_value=rating_value,
                        star_rating=star_rating,
                        video_present=video_present,
                        video_colour=video_colour,
                        video_aspect=video_aspect,
                        video_quality=video_quality,
                        audio_present=audio_present,
                        audio_stereo=audio_stereo,
                        subtitles_type=subtitles_type,
                        audio_described=audio_described,
                        is_premiere=is_premiere,
                        is_new=is_new,
                        previously_shown=previously_shown,
                        review_type=review_type,
                        review_value=review_value
                    )
                )
        logger.info("Saving new programmes list for EPG #%s from path - '%s'", epg_id, xmltv_file)
        # Save remaining
        db.session.bulk_save_objects(items, update_changed_only=False)
        # Commit all updates to channel programmes
        db.session.commit()
        logger.info("Successfully imported %s programmes from path - '%s'", len(items), xmltv_file)

    await run_sync(parse_and_save_programmes)()


async def import_epg_data(config, epg_id):
    epg = await read_config_one_epg(epg_id)
    # Download a new local copy of the EPG
    logger.info("Downloading updated XMLTV file for EPG #%s from url - '%s'", epg_id, epg['url'])
    start_time = time.time()
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    await download_xmltv_epg(epg['url'], xmltv_file)
    execution_time = time.time() - start_time
    logger.info("Updated XMLTV file for EPG #%s was downloaded in '%s' seconds", epg_id, int(execution_time))
    # Read and save EPG data to DB
    logger.info("Importing updated data for EPG #%s", epg_id)
    start_time = time.time()
    await clear_epg_channel_data(epg_id)
    channel_id_list = await store_epg_channels(config, epg_id)
    await store_epg_programmes(config, epg_id, channel_id_list)
    execution_time = time.time() - start_time
    logger.info("Updated data for EPG #%s was imported in '%s' seconds", epg_id, int(execution_time))


async def import_epg_data_for_all_epgs(config):
    for epg in db.session.query(Epg).all():
        await import_epg_data(config, epg.id)


async def read_channels_from_all_epgs(config):
    epgs_channels = {}
    for result in db.session.query(Epg).options(joinedload(Epg.epg_channels)).all():
        epgs_channels[result.id] = []
        for epg_channel in result.epg_channels:
            epgs_channels[result.id].append({
                "channel_id":   epg_channel.channel_id,
                "display_name": epg_channel.name,
                "icon":         epg_channel.icon_url,
            })
    return epgs_channels


# --- Cache ---
async def build_custom_epg(config):
    loop = asyncio.get_event_loop()
    settings = config.read_settings()
    logger.info("Generating custom EPG for TVH based on configured channels.")
    start_time = time.time()
    # Create the root <tv> element of the output XMLTV file
    output_root = ET.Element('tv')
    # Set the attributes for the output root element
    output_root.set('generator-info-name', 'TVH-IPTV-Config')
    output_root.set('source-info-name', 'TVH-IPTV-Config - v0.1')
    # Read programmes from cached source EPG
    configured_channels = []
    all_channel_programmes_data = []
    # for key in settings.get('channels', {}):
    logger.info("   - Building a programme data for each channel.")
    for result in db.session.query(Channel).order_by(Channel.number.asc()).all():
        if result.enabled:
            channel_id = generate_epg_channel_id(result.number, result.name)
            # Read cached image
            image_data, mime_type = await read_base46_image_string(result.logo_base64)
            cache_buster = time.time()
            ext = guess_extension(mime_type)
            logo_url = f"{settings['settings']['app_url']}/tic-api/channels/{result.id}/logo/{cache_buster}{ext}"
            # Populate a channels list
            configured_channels.append({
                'channel_id':   channel_id,
                'display_name': result.name,
                'logo_url':     logo_url,
            })
            db_programmes_query = db.session.query(EpgChannelProgrammes) \
                .options(joinedload(EpgChannelProgrammes.channel)) \
                .filter(and_(EpgChannelProgrammes.channel.has(epg_id=result.guide_id),
                             EpgChannelProgrammes.channel.has(channel_id=result.guide_channel_id)
                             )) \
                .order_by(EpgChannelProgrammes.channel_id.asc(), EpgChannelProgrammes.start.asc())
            # logger.debug(db_programmes_query)
            db_programmes = db_programmes_query.all()
            programmes = []
            logger.info("       - Building programme list for %s - %s.", channel_id, result.name)
            for programme in db_programmes:
                programme_data = {
                    'start':           programme.start,
                    'stop':            programme.stop,
                    'start_timestamp': programme.start_timestamp,
                    'stop_timestamp':  programme.stop_timestamp,
                    'title':           programme.title,
                    'sub-title':       programme.sub_title,
                    'desc':            programme.desc,
                    'series-desc':     programme.series_desc,
                    'country':         programme.country,
                    'icon_url':        programme.icon_url,
                    'categories':      json.loads(programme.categories) if programme.categories else [],
                    'url':             programme.url,
                    'date':            programme.date,
                    'length':          programme.length,
                    'keywords':        json.loads(programme.keywords) if programme.keywords else [],
                    'credits':         json.loads(programme.credits) if programme.credits else {},
                    'episode_num_system': programme.episode_num_system,
                    'episode_num_value':  programme.episode_num_value,
                    'rating_system':   programme.rating_system,
                    'rating_value':    programme.rating_value,
                    'star_rating':     programme.star_rating,
                    'video_present':   programme.video_present,
                    'video_colour':    programme.video_colour,
                    'video_aspect':    programme.video_aspect,
                    'video_quality':   programme.video_quality,
                    'audio_present':   programme.audio_present,
                    'audio_stereo':    programme.audio_stereo,
                    'subtitles_type':  programme.subtitles_type,
                    'audio_described': programme.audio_described,
                    'is_premiere':     programme.is_premiere,
                    'is_new':          programme.is_new,
                    'previously_shown': programme.previously_shown,
                    'review_type':     programme.review_type,
                    'review_value':    programme.review_value
                }
                programmes.append(programme_data)
            all_channel_programmes_data.append({
                'channel':    channel_id,
                'tags':       [tag.name for tag in result.tags],
                'programmes': programmes
            })
        await asyncio.sleep(.1)
    # Loop over all configured channels
    logger.info("   - Generating XML channel info.")
    for channel_info in configured_channels:
        # Create a <channel> element for a TV channel
        channel = ET.SubElement(output_root, 'channel')
        channel.set('id', str(channel_info['channel_id']))
        # Add a <display-name> element to the <channel> element
        display_name = ET.SubElement(channel, 'display-name')
        display_name.text = channel_info['display_name'].strip()
        # Add a <icon> element to the <channel> element
        icon = ET.SubElement(channel, 'icon')
        icon.set('src', channel_info['logo_url'])
        # Add a <live> element to the <channel> element
        live = ET.SubElement(channel, 'live')
        live.text = 'true'
        # Add a <active> element to the <channel> element
        active = ET.SubElement(channel, 'active')
        active.text = 'true'
        await asyncio.sleep(.1)
    # Loop through all <programme> elements returned
    logger.info("   - Generating XML channel programme data.")
    for channel_programmes_data in all_channel_programmes_data:
        for epg_channel_programme in channel_programmes_data.get('programmes', []):
            # Create a <programme> element for the output file and copy the attributes from the input programme
            output_programme = ET.SubElement(output_root, 'programme')
            # Build programmes from DB data (manually create attributes etc.
            if epg_channel_programme['start']:
                output_programme.set('start', epg_channel_programme['start'])
            if epg_channel_programme['stop']:
                output_programme.set('stop', epg_channel_programme['stop'])
            if epg_channel_programme['start_timestamp']:
                output_programme.set('start_timestamp', epg_channel_programme['start_timestamp'])
            if epg_channel_programme['stop_timestamp']:
                output_programme.set('stop_timestamp', epg_channel_programme['stop_timestamp'])
            # Set the "channel" ident here
            output_programme.set('channel', str(channel_programmes_data.get('channel')))
            # Loop through all child elements of the input programme and copy them to the output programme
            for child in ['title', 'sub-title', 'desc', 'series-desc', 'country']:
                # Copy all other child elements to the output programme if they exist
                if child in epg_channel_programme and epg_channel_programme[child] is not None:
                    output_child = ET.SubElement(output_programme, child)
                    output_child.text = epg_channel_programme[child]
            
            # Add URL if available
            if epg_channel_programme.get('url'):
                output_child = ET.SubElement(output_programme, 'url')
                output_child.text = epg_channel_programme['url']
            
            # Add date if available
            if epg_channel_programme.get('date'):
                output_child = ET.SubElement(output_programme, 'date')
                output_child.text = epg_channel_programme['date']
            
            # Add length if available
            if epg_channel_programme.get('length'):
                output_child = ET.SubElement(output_programme, 'length')
                if ' ' in epg_channel_programme['length']:
                    length_parts = epg_channel_programme['length'].split(' ', 1)
                    output_child.text = length_parts[0]
                    if len(length_parts) > 1:
                        output_child.set('units', length_parts[1])
                else:
                    output_child.text = epg_channel_programme['length']
                    output_child.set('units', 'minutes')
            
            # Add credits if available
            if epg_channel_programme.get('credits'):
                credits_elem = ET.SubElement(output_programme, 'credits')
                for credit_type, credit_list in epg_channel_programme['credits'].items():
                    for credit_name in credit_list:
                        credit_elem = ET.SubElement(credits_elem, credit_type)
                        credit_elem.text = credit_name
            
            # Add episode number if available
            if epg_channel_programme.get('episode_num_system') and epg_channel_programme.get('episode_num_value'):
                episode_elem = ET.SubElement(output_programme, 'episode-num')
                episode_elem.set('system', epg_channel_programme['episode_num_system'])
                episode_elem.text = epg_channel_programme['episode_num_value']
            
            # Add rating if available
            if epg_channel_programme.get('rating_system') and epg_channel_programme.get('rating_value'):
                rating_elem = ET.SubElement(output_programme, 'rating')
                rating_elem.set('system', epg_channel_programme['rating_system'])
                value_elem = ET.SubElement(rating_elem, 'value')
                value_elem.text = epg_channel_programme['rating_value']
            
            # Add star rating if available
            if epg_channel_programme.get('star_rating'):
                star_rating_elem = ET.SubElement(output_programme, 'star-rating')
                value_elem = ET.SubElement(star_rating_elem, 'value')
                value_elem.text = epg_channel_programme['star_rating']
            
            # Add video information if available
            video_info = {}
            if epg_channel_programme.get('video_present') is not None:
                video_info['present'] = 'yes' if epg_channel_programme['video_present'] else 'no'
            if epg_channel_programme.get('video_colour') is not None:
                video_info['colour'] = 'yes' if epg_channel_programme['video_colour'] else 'no'
            if epg_channel_programme.get('video_aspect'):
                video_info['aspect'] = epg_channel_programme['video_aspect']
            if epg_channel_programme.get('video_quality'):
                video_info['quality'] = epg_channel_programme['video_quality']
            
            if video_info:
                video_elem = ET.SubElement(output_programme, 'video')
                for key, value in video_info.items():
                    child_elem = ET.SubElement(video_elem, key)
                    child_elem.text = value
            
            # Add audio information if available
            audio_info = {}
            if epg_channel_programme.get('audio_present') is not None:
                audio_info['present'] = 'yes' if epg_channel_programme['audio_present'] else 'no'
            if epg_channel_programme.get('audio_stereo'):
                audio_info['stereo'] = epg_channel_programme['audio_stereo']
            
            if audio_info:
                audio_elem = ET.SubElement(output_programme, 'audio')
                for key, value in audio_info.items():
                    child_elem = ET.SubElement(audio_elem, key)
                    child_elem.text = value
            
            # Add subtitles if available
            if epg_channel_programme.get('subtitles_type'):
                subtitles_elem = ET.SubElement(output_programme, 'subtitles')
                subtitles_elem.set('type', epg_channel_programme['subtitles_type'])
            
            # Add audio description if available
            if epg_channel_programme.get('audio_described'):
                ET.SubElement(output_programme, 'audio-described')
            
            # Add premiere flag if available
            if epg_channel_programme.get('is_premiere'):
                ET.SubElement(output_programme, 'premiere')
            
            # Add new flag if available
            if epg_channel_programme.get('is_new'):
                ET.SubElement(output_programme, 'new')
            
            # Add previously shown if available
            if epg_channel_programme.get('previously_shown'):
                prev_elem = ET.SubElement(output_programme, 'previously-shown')
                prev_elem.set('start', epg_channel_programme['previously_shown'])
            
            # Add review if available
            if epg_channel_programme.get('review_value'):
                review_elem = ET.SubElement(output_programme, 'review')
                if epg_channel_programme.get('review_type'):
                    review_elem.set('type', epg_channel_programme['review_type'])
                review_elem.text = epg_channel_programme['review_value']
            
            # If we have a programme icon, add it
            if epg_channel_programme['icon_url']:
                output_child = ET.SubElement(output_programme, 'icon')
                output_child.set('src', epg_channel_programme['icon_url'])
                output_child.set('height', "")
                output_child.set('width', "")
            
            # Add keywords
            if epg_channel_programme.get('keywords'):
                for keyword in epg_channel_programme['keywords']:
                    keyword_elem = ET.SubElement(output_programme, 'keyword')
                    keyword_elem.text = keyword
                    keyword_elem.set('lang', 'en')
            
            # Loop through all categories for this programme and add them as "category" child elements
            if epg_channel_programme['categories']:
                for category in epg_channel_programme['categories']:
                    output_child = ET.SubElement(output_programme, 'category')
                    output_child.text = category
                    output_child.set('lang', 'en')
            # Loop through all tags for this channel and add them as "category" child elements
            for tag in channel_programmes_data.get('tags', []):
                output_child = ET.SubElement(output_programme, 'category')
                output_child.text = tag
                output_child.set('lang', 'en')
        await asyncio.sleep(.1)
    # Create an XML file and write the output root element to it
    logger.info("   - Writing out XMLTV file.")
    output_tree = ET.ElementTree(output_root)
    ET.indent(output_tree, space="\t", level=0)
    custom_epg_file = os.path.join(config.config_path, "epg.xml")
    await loop.run_in_executor(None, output_tree.write, custom_epg_file, 'UTF-8', True)
    execution_time = time.time() - start_time
    logger.info("The custom XMLTV EPG file for TVH was generated in '%s' seconds", int(execution_time))


# --- Online Metadata ---
async def search_tmdb_for_movie(api_key, title, cache, lock, semaphore):
    async with semaphore:
        async with lock:
            if 'tmdb' not in cache:
                cache['tmdb'] = {}
            if title in cache['tmdb']:
                logger.debug("       - Fetching data for program '%s' from TMDB. [CACHED]", title)
                return cache['tmdb'][title]
        search_url = f'https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={title}'
        async with aiohttp.ClientSession() as session:
            async with session.get(search_url) as response:
                if response.status == 200:
                    results = (await response.json()).get('results', [])
                    if results:
                        async with lock:
                            cache['tmdb'][title] = results[0]  # Cache the first search result
                        logger.debug("       - Fetching data for program '%s' from TMDB. [FETCHED]", title)
                        return results[0]
        async with lock:
            cache['tmdb'][title] = None  # Cache None if no results found
        logger.debug("       - Fetching data for program '%s' from TMDB. [NONE]", title)
        return None


async def search_google_images(title, cache, lock, semaphore):
    async with semaphore:
        async with lock:
            if 'google_images' not in cache:
                cache['google_images'] = {}
            if title in cache['google_images']:
                logger.debug("       - Fetching data for program '%s' from Google Images. [CACHED]", title)
                return cache['google_images'][title]

        search_query = f'"{title}" television show'
        encoded_query = quote(search_query)
        search_url = f'https://www.google.com/search?tbm=isch&safe=active&tbs=isz:m&q={encoded_query}'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(search_url, headers=headers) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    images = soup.find_all('img')
                    if images:
                        # The first image might be the Google logo, so we take the second one
                        image_url = images[1]['src']
                        async with lock:
                            cache['google_images'][title] = image_url  # Cache the first image URL
                        logger.debug("       - Fetching data for program '%s' from Google Images. [FETCHED]", title)
                        return image_url

        async with lock:
            cache['google_images'][title] = None  # Cache None if no results found
        logger.debug("       - Fetching data for program '%s' from Google Images. [NONE]", title)
        return None


async def update_programme_with_online_data(settings, programme, categories, cache, lock, semaphore):
    updated = False
    title = programme.title
    categories = [category.lower() for category in categories]

    # Fetch updated data from TMDB
    if not (programme.sub_title or programme.desc or programme.icon_url):
        if settings['settings'].get('epgs', {}).get('enable_tmdb_metadata'):
            api_key = settings['settings'].get('epgs', {}).get('tmdb_api_key', '')
            tmdb_data = await search_tmdb_for_movie(api_key, title, cache, lock, semaphore)
            if tmdb_data:
                # Update programme with fetched data if fields are missing
                if not programme.sub_title:
                    programme.sub_title = tmdb_data.get('title')
                if not programme.desc:
                    programme.desc = tmdb_data.get('overview')
                if not programme.icon_url:
                    programme.icon_url = f"https://image.tmdb.org/t/p/w500{tmdb_data.get('poster_path')}"

    # Fetch icon_url from Google Images if still missing
    if not programme.icon_url:
        if settings['settings'].get('epgs', {}).get('enable_google_image_search_metadata'):
            image_url = await search_google_images(title, cache, lock, semaphore)
            if image_url:
                programme.icon_url = image_url

    return programme


async def update_programmes_concurrently(settings, programmes, cache, lock):
    semaphore = asyncio.Semaphore(10)

    async def update_wrapper(programme):
        categories = json.loads(programme.categories)
        return await update_programme_with_online_data(settings, programme, categories, cache, lock, semaphore)

    tasks = [update_wrapper(programme) for programme in programmes]
    updated_programmes = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(updated_programmes):
        if isinstance(result, Exception):
            logger.error(f"Error updating programme: {result}")
        else:
            programmes[i] = result

    return programmes


async def update_channel_epg_with_online_data(config):
    settings = config.read_settings()
    update_with_online_data = False
    if settings['settings'].get('epgs', {}).get('enable_tmdb_metadata'):
        update_with_online_data = True
    if settings['settings'].get('epgs', {}).get('enable_google_image_search_metadata'):
        update_with_online_data = True
    if not update_with_online_data:
        return
    start_time = time.time()
    cache = {}
    lock = asyncio.Lock()
    logger.info("Update EPG with missing data from online sources for each configured channel.")
    for result in db.session.query(Channel).order_by(Channel.number.asc()).all():
        if result.enabled:
            channel_id = generate_epg_channel_id(result.number, result.name)
            db_programmes_query = db.session.query(EpgChannelProgrammes) \
                .options(joinedload(EpgChannelProgrammes.channel)) \
                .filter(and_(EpgChannelProgrammes.channel.has(epg_id=result.guide_id),
                             EpgChannelProgrammes.channel.has(channel_id=result.guide_channel_id)
                             )) \
                .order_by(EpgChannelProgrammes.channel_id.asc(), EpgChannelProgrammes.start.asc())
            db_programmes = db_programmes_query.all()
            logger.info("   - Updating programme list for %s - %s.", channel_id, result.name)
            programmes = await update_programmes_concurrently(settings, db_programmes, cache, lock)
            # Save all new
            db.session.bulk_save_objects(programmes)
            # Commit all updates to channel programmes
            db.session.commit()
    execution_time = time.time() - start_time
    logger.info("Updating online EPG data for configured channels took '%s' seconds", int(execution_time))


# --- TVH Functions ---
async def run_tvh_epg_grabbers(config):
    # Trigger a re-grab of the EPG in TVH
    async with await get_tvh(config) as tvh:
        await tvh.run_internal_epg_grabber()
