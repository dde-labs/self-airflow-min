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


class Stream(BaseModel):
    stream_id: str
    process_groups: list[ProcessGroup] = Field(default_factory=list)
    active: bool = Field(default=True)


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
