#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import io

from backend.api import blueprint
from quart import request, jsonify, current_app, send_file

from backend.auth import admin_auth_required
from backend.channels import read_config_all_channels, add_new_channel, read_config_one_channel, update_channel, \
    delete_channel, add_bulk_channels, queue_background_channel_update_tasks, read_channel_logo, add_channels_from_groups


@blueprint.route('/tic-api/channels/get', methods=['GET'])
@admin_auth_required
async def api_get_channels():
    channels_config = await read_config_all_channels()
    return jsonify(
        {
            "success": True,
            "data":    channels_config
        }
    )


@blueprint.route('/tic-api/channels/new', methods=['POST'])
@admin_auth_required
async def api_add_new_channel():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    await add_new_channel(config, json_data)
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/settings/<channel_id>', methods=['GET'])
@admin_auth_required
async def api_get_channel_config(channel_id):
    channel_config = read_config_one_channel(channel_id)
    return jsonify(
        {
            "success": True,
            "data":    channel_config
        }
    )


@blueprint.route('/tic-api/channels/settings/<channel_id>/save', methods=['POST'])
@admin_auth_required
async def api_set_config_channels(channel_id):
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    await update_channel(config, channel_id, json_data)
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/settings/multiple/save', methods=['POST'])
@admin_auth_required
async def api_set_config_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    for channel_id in json_data.get('channels', {}):
        channel = json_data['channels'][channel_id]
        await update_channel(config, channel_id, channel)
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/settings/multiple/add', methods=['POST'])
@admin_auth_required
async def api_add_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    await add_bulk_channels(config, json_data.get('channels', []))
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/settings/multiple/delete', methods=['POST'])
@admin_auth_required
async def api_delete_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    current_app.logger.warning(json_data)
    
    for channel_id in json_data.get('channels', {}):
        await delete_channel(config, channel_id)
    
    # Queue background tasks to update TVHeadend
    await queue_background_channel_update_tasks(config)
    
    return jsonify({
        "success": True
    })


@blueprint.route('/tic-api/channels/settings/<channel_id>/delete', methods=['DELETE'])
@admin_auth_required
async def api_delete_config_channels(channel_id):
    config = current_app.config['APP_CONFIG']
    await delete_channel(config, channel_id)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/<channel_id>/logo/<file_placeholder>', methods=['GET'])
async def api_get_channel_logo(channel_id, file_placeholder):
    image_base64_string, mime_type = await read_channel_logo(channel_id)
    # Convert to a BytesIO object for sending file
    image_io = io.BytesIO(image_base64_string)
    image_io.seek(0)
    # Return file blob
    return await send_file(image_io, mimetype=mime_type)

@blueprint.route('/tic-api/channels/settings/groups/add', methods=['POST'])
@admin_auth_required
async def api_add_channels_from_groups():
    json_data = await request.get_json()
    groups = json_data.get('groups', [])
    
    if not groups:
        return jsonify({
            "success": False,
            "message": "No groups provided"
        }), 400
    
    config = current_app.config['APP_CONFIG']
    
    # This function needs to be implemented in the channels module
    # It should add all channels from the specified groups
    added_count = await add_channels_from_groups(config, groups)
    
    await queue_background_channel_update_tasks(config)
    
    return jsonify({
        "success": True,
        "data": {
            "added_count": added_count
        }
    })

