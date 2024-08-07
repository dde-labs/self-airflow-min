from pydantic import BaseModel, Field, ConfigDict


class CustomModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    def __getattr__(self, item):
        for field, meta in self.model_fields.items():
            if meta.alias == item:
                return getattr(self, field)
        return super().__getattr__(item)


class Source(CustomModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str | None = Field(default=None, alias='schema')
    table_name: str | None = Field(default=None, alias='table')


class Target(CustomModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str | None = Field(default=None, alias='schema')
    table_name: str | None = Field(default=None, alias='table')


class Process(CustomModel):
    model_config = ConfigDict(populate_by_name=True)

    priority: int = Field(default=99)
    id: str = Field(alias='process_id')
    type: int = Field(default=99)
    source: Source = Field(default_factory=Source)
    target: Target = Field(default_factory=Target)
    active: bool = Field(default=True)
    sla: int = Field(default=30)


class ProcessGroup(CustomModel):
    model_config = ConfigDict(populate_by_name=True)

    priority: int = Field(default=99)
    id: str = Field(alias='process_group_id')
    processes: list[Process] = Field(default_factory=list)
    active: bool = Field(default=True)

    def priorities(self) -> list[list[Process]]:
        """Return a list of list of Process that group by a priority value."""
        if not self.processes:
            return []
        return [
            priority
            for priority in (
                [y for y in self.processes if y.priority == p]
                for p in set(map(lambda x: x.priority, self.processes))
            )
        ]


class Stream(CustomModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias='stream_id')
    process_groups: list[ProcessGroup] = Field(default_factory=list)
    active: bool = Field(default=True)


class StreamBatch(CustomModel):
    id: str
    alias: str


class Batch(CustomModel):
    batch_id: str
    streams: list[StreamBatch] = Field(default_factory=list)
    deps: str | None = Field(default=None)


class Deployment(CustomModel):
    manual_streams: list[str] = Field(default_factory=list)
    streams: list[str] = Field(default_factory=list)
    batches: list[str] = Field(default_factory=list)
    processes: list[str] = Field(default_factory=list)


if __name__ == '__main__':
    data = Stream.model_validate(obj={
        'stream_id': 'S_AD_D',
        'process_groups': [
            {
                'priority': 1,
                'id': 'PG_AD_PROCESS_GROUP_1_D',
                'processes': [
                    {
                        'priority': 1,
                        'id': 'P_AD_PROCESS_1_1_D_99',
                        'type': 1,
                        'source': {
                            'schema': "SRC_SCHEMA",
                            'table': "SRC_TABLE",
                        },
                        'target': {
                            'schema': "SRC_SCHEMA",
                            'table': "SRC_TABLE",
                        },
                    }
                ]
            },
            {
                'priority': 2,
                'id': 'PG_AD_PROCESS_GROUP_2_D',
            }
        ]
    })
    print(data)

    process = Process.model_validate(obj={
        'process_id': 'P_AD_PROCESS_1_1_D_99',
        'type': 1,
        'source': {
            'schema': "SRC_SCHEMA",
            'table': "SRC_TABLE",
        },
        'target': {
            'schema': "SRC_SCHEMA",
            'table': "SRC_TABLE",
        },
    })
    print(process)

    deploy = Deployment.model_validate(obj={
        'streams': [
            's_ad_d',
            's_fm_d',
        ],
        'batches': [
            'batch_1',
        ]
    })
    print(deploy)
