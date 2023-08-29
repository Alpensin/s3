from typing import List

from pydantic import BaseModel, constr, Field

from config import settings

UID = constr(min_length=24, max_length=32)


class FileName(BaseModel):
    uid: UID = Field(..., example="dfa6fd3a013b439f8d5d5db2e672d0aa")
    endpoint: str = ""


class FileInfo(BaseModel):
    length: int
    mimetype: str = settings.images_mime_type
    name: str = "photo_index.jpeg"
    status: int = 0
    tags: List = []
    type: str = "public"
    upload_date: str = Field(..., example="2019-06-21T08:43:41.734+03:00")


image_read_responses = {
    200: {
        "description": "Изображение в base64",
        "content": {
            settings.images_mime_type: {"example": "Example values are not available for image/jpeg media types."}
        },
    }
}

