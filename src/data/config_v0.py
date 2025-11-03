from config_dataclasses import GraphConfig

config = GraphConfig(
    fields_to_keep={
        # Common fields for ALL node types
        'common': [
            'id',
            'name',
        ],
       
        # AgriFarm specific fields
        'AgriFarm': [
            #'location',
        ],
       
        # AgriParcel specific fields
        'AgriParcel': [
            #'location',
            # 'colture',
            # 'irrigationSystemType',
        ],
       
        # Device specific fields
        'Device': [
            #'location',
            # 'value',
            # 'controlledProperty',
            # 'deviceCategory',
            # 'x', 'y', 'z',
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
        'Measurement'
       
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