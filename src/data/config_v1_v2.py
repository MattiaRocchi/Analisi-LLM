from config_dataclasses import GraphConfig

# V1 and V2 Configuration - detailed properties
config = GraphConfig(
    fields_to_keep={
        # Common fields for ALL node types
        'common': [
            'id',
        ],
       
        # AgriFarm specific fields
        'AgriFarm': [
            'location',
            # Add any other AgriFarm-specific properties here
        ],
       
        # AgriParcel specific fields
        'AgriParcel': [
            'location',
            'colture',
            'irrigationSystemType',
        ],
       
        # Device specific fields
        'Device': [
            'location',
            'value',
            'controlledProperty',
            'deviceCategory',     
            'x',
            'y',
            'z',
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
        'type',
       
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