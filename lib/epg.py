import os
import xml.etree.ElementTree as ET
import xml.parsers.expat

import requests as requests

from lib.config import update_yaml, read_yaml

CHUNK_SIZE = 64 * 1024


def download_xmltv_epg(url, output):
    print(f"Downloading EPG from url - '{url}'")
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    with requests.get(url, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        with open(output, 'wb') as f:
            for chunk in r.iter_content(chunk_size=128):
                f.write(chunk)


def import_epg_data(config, epg_id):
    settings = config.read_settings()
    epg_info = settings['epgs'][epg_id]
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    # Download a new local copy of the EPG
    download_xmltv_epg(epg_info.get('url', ''), xmltv_file)
    # Parse the XMLTV file for channels and cache them
    parse_xmltv_for_channels(config, epg_id)


def import_all_epg_data(config):
    settings = config.read_settings()
    for key in settings['epgs']:
        epg_info = settings['epgs'][key]
        if epg_info['enabled']:
            xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{key}.xml")
            download_xmltv_epg(epg_info.get('url', ''), xmltv_file)


def read_epgs_config(config, epg_id=None):
    settings = config.read_settings()
    if epg_id is None:
        return settings['epgs']
    return settings['epgs'].get(epg_id, {})


def read_channels_config(config, channel_number=None):
    settings = config.read_settings()
    if channel_number is None:
        return settings['channels']
    # When fetching for a single channel, add some extra details from the playlist
    channel_info = settings['channels'].get(channel_number, {})
    for source_id in channel_info.get('sources', {}):
        source = channel_info['sources'][source_id]
        playlist_info = settings.get('playlists', {}).get(source['playlist_id'])
        source['playlist_name'] = playlist_info['name']
        source['playlist_enabled'] = playlist_info['enabled']
    return channel_info


def prune_playlist_from_channel_sources(config, playlist_id):
    channels = read_channels_config(config)
    for channel_id in channels.copy():
        channel = channels[channel_id]
        for source_id in channel['sources'].copy():
            source = channel['sources'][source_id]
            if source.get('playlist_id') == playlist_id:
                del channel['sources'][source_id]


def remove_channel_from_channels_config(config, channel_number):
    settings = config.read_settings()
    if channel_number in settings['channels']:
        del settings['channels'][channel_number]


def read_channels_from_all_epgs(config):
    settings = config.read_settings()
    epgs_channels = {}
    for key in settings['epgs']:
        epgs_channels[key] = read_channels_from_epg_cache(config, key)
    return epgs_channels


def read_channels_from_epg_cache(config, epg_id):
    epg_cache = read_yaml(os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.yml"))
    return epg_cache.get('channels', [])


def parse_xmltv_for_channels(config, epg_id):
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    if not os.path.exists(xmltv_file):
        # TODO: Add error logging here
        print(f"No such file '{xmltv_file}'")
        return []
    print(f"Reading EPG available channels from path - '{xmltv_file}'")
    tree = ET.parse(xmltv_file)
    root = tree.getroot()
    # Find all <channel> elements in the XMLTV file
    channels = root.findall('.//channel')
    # Cache channel data in a yaml file
    epg_channels = {
        'channels': []
    }
    for channel in channels:
        channel_id = channel.get('id')
        display_name = channel.find('display-name').text
        icon = ''
        icon_attrib = channel.find('icon')
        if icon_attrib:
            icon = icon_attrib.attrib.get('src', '')
        print(f"Channel ID: '{channel_id}', Display Name: '{display_name}', Icon: {icon}")
        epg_channels['channels'].append({
            'channel_id':   channel_id,
            'display_name': display_name,
            'icon':         icon,
        })
    epg_yaml_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.yml")
    update_yaml(epg_yaml_file, epg_channels)
    return channels


def parse_xmltv_for_programmes_for_channel(config, epg_id, channel_id):
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    if not os.path.exists(xmltv_file):
        # TODO: Add error logging here
        print(f"No such file '{xmltv_file}'")
        return []
    print(f"Reading EPG programme for channel ID '{channel_id}' from path - '{xmltv_file}'")
    input_file = ET.parse(xmltv_file)
    root = input_file.getroot()
    channel_programmes = []
    # Loop through all <programme> elements in the input file
    for programme in root.findall('.//programme'):
        # Check if the programme is for the desired channel
        if str(programme.get('channel')) == str(channel_id):
            channel_programmes.append(programme)
    return channel_programmes


def update_custom_epg(config):
    settings = config.read_settings()

    # Create the root <tv> element of the output XMLTV file
    output_root = ET.Element('tv')

    # Set the attributes for the output root element
    output_root.set('generator-info-name', 'TVH-IPTV-Config')
    output_root.set('source-info-name', 'TVH-IPTV-Config - v0.1')

    # Read programmes from cached source EPG
    configured_channels = []
    discovered_programmes = []
    for key in settings.get('channels', {}):
        if settings.get('channels', {}).get(key).get('enabled'):
            # Populate a channels list
            configured_channels.append({
                'channel':      key,
                'display_name': settings.get('channels', {}).get(key).get('name', ''),
                'logo_url':     settings.get('channels', {}).get(key).get('logo_url', ''),
            })
            epg_id = settings.get('channels', {}).get(key).get('guide', {}).get('epg_id')
            channel_id = settings.get('channels', {}).get(key).get('guide', {}).get('channel_id')
            discovered_programmes.append({
                'channel':    key,
                'programmes': parse_xmltv_for_programmes_for_channel(config, epg_id, channel_id)
            })

    # Loop over all configured channels
    for channel_info in configured_channels:
        # Create a <channel> element for a TV channel
        channel = ET.SubElement(output_root, 'channel')
        channel.set('id', channel_info.get('channel'))
        # Add a <display-name> element to the <channel> element
        display_name = ET.SubElement(channel, 'display-name')
        display_name.text = channel_info.get('display_name')
        # Add a <icon> element to the <channel> element
        icon = ET.SubElement(channel, 'icon')
        icon.set('src', channel_info.get('logo_url'))

    # Loop through all <programme> elements returned
    for channel_programme_info in discovered_programmes:
        for programme in channel_programme_info['programmes']:
            # Create a <programme> element for the output file and copy the attributes from the input programme
            output_programme = ET.SubElement(output_root, 'programme')
            output_programme.attrib = programme.attrib
            # Set the channel number here
            output_programme.set('channel', channel_programme_info['channel'])
            # Loop through all child elements of the input programme and copy them to the output programme
            for child in programme:
                # Skip the <channel> element, which is already set by the output_programme.attrib line
                if child.tag == 'channel':
                    continue
                # Copy all other child elements to the output programme
                output_child = ET.SubElement(output_programme, child.tag)
                output_child.text = child.text

    # Create an XML file and write the output root element to it
    output_tree = ET.ElementTree(output_root)
    ET.indent(output_tree, space="\t", level=0)
    output_tree.write(os.path.join(config.config_path, "epg.xml"), encoding='UTF-8', xml_declaration=True)
