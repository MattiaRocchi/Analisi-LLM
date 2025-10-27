"""
Configuration file for Graph V0 (minimal structure)
V0 includes only basic schema information without detailed properties.

HOW TO USE:
- Keep the properties you want in the lists below
- Comment out or remove properties you don't want
- 'common' fields apply to ALL node types
- Type-specific fields apply only to that node type
"""

from config_dataclasses import GraphConfig

# V0 Configuration - minimal structure
config = GraphConfig(
    fields_to_keep={
        # Common fields for ALL node types
        'common': [
            'id',      # URN identifier
            'name',    # Human-readable name
            # 'type',  # Entity type (uncomment if needed in V0)
        ],
       
        # AgriFarm specific fields
        'AgriFarm': [
            'location',  # Geographic location
            # Add more fields here if needed
        ],
       
        # AgriParcel specific fields
        'AgriParcel': [
            'location',  # Geographic location
            # 'colture',              # Crop type (uncomment if needed)
            # 'irrigationSystemType', # Irrigation system (uncomment if needed)
        ],
       
        # Device specific fields
        'Device': [
            'location',  # Geographic location
            # 'value',              # Current value (uncomment if needed)
            # 'controlledProperty', # What the device measures (uncomment if needed)
            # 'deviceCategory',     # Device category (uncomment if needed)
            # 'x', 'y', 'z',       # Coordinates (uncomment if needed)
        ]
    },
    fields_to_exclude=[
        # Timestamps
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
        'description',
       
        # Relationships (handled separately in edge processing)
        'belongsTo',
        'hasDevice',
        'hasAgriParcel',
        'hasMeasurement',
       
        # Properties better suited for V1/V2
        'type',
        'value',
        'x', 'y', 'z',
        'controlledProperty',
        'deviceCategory',
        'colture',
        'irrigationSystemType',
    ]
)