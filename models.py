from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


# Class for music bands
class Band(BaseModel):
    name: str = Field(max_length=50)                # name of the band
    date_of_formation: Optional[datetime] = None    # date that the band was formed/created
    members: int = Field(gt=1)                      # number of members
    albums: List[str]                               # a list of albums that the band has released

    # Return data as json dictionary
    def to_json(self):
        return self.model_dump()
