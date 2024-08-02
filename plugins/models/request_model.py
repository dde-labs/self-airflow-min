import pydantic


class InputData(pydantic.BaseModel):
    columns: list[int]
    index: list[int]
    data: list[list[int]]

    @pydantic.field_validator("columns")
    def columns_valid(cls, field_value) -> None:
        """Validator to check whether columns are valid"""
        if len(field_value) != 23:
            raise ValueError("Columns should be of length 23")

        for x in field_value:
            if x not in range(0, 23):
                raise ValueError("Columns should be in range 0-22")

        return field_value


class Request(pydantic.BaseModel):
    input_data: InputData
