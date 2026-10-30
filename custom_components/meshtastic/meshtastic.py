"""Register gateway and channel entities with their config entry."""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.logbook import DOMAIN as LOGBOOK_DOMAIN

from .entity import GatewayChannelEntity, GatewayDirectMessageEntity, GatewayEntity

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from .data import MeshtasticConfigEntry


async def async_setup_entry(
    hass: HomeAssistant,
    entry: MeshtasticConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    client = entry.runtime_data.client
    gateway_node = await client.async_get_own_node()
    gateway_entity = GatewayEntity(
        config_entry_id=entry.entry_id,
        node=gateway_node["num"],
        long_name=gateway_node["user"]["longName"],
        short_name=gateway_node["user"]["shortName"],
        local_config=await client.async_get_node_local_config(),
        module_config=await client.async_get_node_module_config(),
    )
    has_logbook = LOGBOOK_DOMAIN in hass.config.all_components
    direct_messages = GatewayDirectMessageEntity(
        config_entry_id=entry.entry_id,
        gateway_node=gateway_node["num"],
        gateway_entity=gateway_entity,
        has_logbook=has_logbook,
    )
    channels = [
        GatewayChannelEntity(
            config_entry_id=entry.entry_id,
            gateway_node=gateway_node["num"],
            gateway_entity=gateway_entity,
            index=channel["index"],
            name=channel["settings"]["name"],
            primary=channel["role"] == "PRIMARY",
            secondary=channel["role"] == "SECONDARY",
            settings=channel["settings"],
            has_logbook=has_logbook,
        )
        for channel in await client.async_get_channels()
        if channel["role"] != "DISABLED"
    ]
    async_add_entities([gateway_entity, direct_messages, *channels])
