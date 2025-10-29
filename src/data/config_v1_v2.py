"""
Configuration file for Graph V1 and V2 (full structure with properties)
V1/V2 include detailed properties for each node type.

HOW TO USE:
- Keep the properties you want in the lists below
- Comment out or remove properties you don't want
- 'common' fields apply to ALL node types
- Type-specific fields apply only to that node type
"""

from config_dataclasses import GraphConfig

# V1 and V2 Configuration - detailed properties
config = GraphConfig(
    fields_to_keep={
        # Common fields for ALL node types
        'common': [
            'id',      # URN identifier
            'type',    # Entity type
        ],
       
        # AgriFarm specific fields
        'AgriFarm': [
            'location',  # Geographic location (GeoJSON)
            # Add any other AgriFarm-specific properties here
        ],
       
        # AgriParcel specific fields
        'AgriParcel': [
            'location',              # Geographic location (GeoJSON)
            'colture',               # Crop/culture type
            'irrigationSystemType',  # Type of irrigation system
        ],
       
        # Device specific fields
        'Device': [
            'location',           # Geographic location (GeoJSON)
            'value',              # Current sensor value
            'controlledProperty', # What property the device measures (temperature, humidity, etc.)
            'deviceCategory',     # Category of device (sensor, actuator, etc.)
            'x',                  # X coordinate
            'y',                  # Y coordinate
            'z',                  # Z coordinate
        ]
    },
    fields_to_exclude=[
        # Timestamps (unless specifically needed)
        'dateCreated',
        'dateModified',
        'dateObserved',
        'timestamp_kafka',
        'unixtimestampCreated',
        'unixtimestampModified',
        'timestamp_subscription',
       
        # Metadata
        'domain',
        'namespace',
       
        # Relationships (handled separately in edge processing)
        'belongsTo',
        'hasDevice',
        'hasAgriParcel',
       
        # Add any other fields you want to always exclude
    ]
)