# Please complete the TODO items in the code

from dataclasses import asdict, dataclass
import json
import random

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise6", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

uri_summary_table = app.Table("uri_summary", default=str)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
  
    async for ce in clickevents.group_by(ClickEvent.uri):
      
      uri_summary_table[ce.number] += ce.number
      print(f'{ce.uri}: uri')


if __name__ == "__main__":
    app.main()
