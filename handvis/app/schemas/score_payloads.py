"""素材评分 API 的请求与响应协议。"""

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ScoreContextPayload(BaseModel):
    ep_dir: str
    item_name: str

    @field_validator("ep_dir", "item_name")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        """清理必填文本，避免生成空评分路径。"""
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("字段不能为空")
        return cleaned


class ScoreContextResponse(BaseModel):
    oss_path: str
    file_name: str


class ScoreCreatePayload(ScoreContextPayload):
    is_ego: bool
    score: float = Field(ge=0.0, le=5.0, multiple_of=0.5)
    comment: str = Field(default="", max_length=4000)


class ScoreRecordResponse(ScoreContextResponse):
    model_config = ConfigDict(from_attributes=True)

    id: int
    is_ego: bool
    score: float
    comment: str | None
