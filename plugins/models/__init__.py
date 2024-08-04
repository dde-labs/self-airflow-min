from pydantic import BaseModel, Field


class Source(BaseModel):
    schema: str | None = Field(default=None)
    table: str


class Target(BaseModel):
    schema: str | None = Field(default=None)
    table: str


class Process(BaseModel):
    priority: int = Field(default=99)
    id: str
    type: int
    source: Source
    target: Target
    active: bool = Field(default=True)
    sla: int = Field(default=30)


class ProcessGroup(BaseModel):
    priority: int = Field(default=99)
    id: str
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


class Stream(BaseModel):
    stream_id: str
    process_groups: list[ProcessGroup] = Field(default_factory=list)
    active: bool = Field(default=True)


class StreamBatch(BaseModel):
    id: str
    alias: str


class Batch(BaseModel):
    batch_id: str
    streams: list[StreamBatch] = Field(default_factory=list)
    deps: str | None = Field(default=None)


class Deployment(BaseModel):
    streams: list[str] = Field(default_factory=list)
    batches: list[str] = Field(default_factory=list)


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
