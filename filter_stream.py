
# Please complete the TODO items in the code

# To run this: kafka-console-consumer --bootstrap-server localhost:9092 --topic com.udacity.streams.clickevents

from dataclasses import asdict, dataclass
import json

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise4", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)
popular_uris_topic = app.topic(
    "com.udacity.streams.clickevents.popular",
    key_type=str,
    value_type=ClickEvent,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for event in clickevents.filter(lambda x: x.number >=600):
        await clickevents_topic.send(key=event.uri, value=event)
    
    

if __name__ == "__main__":
    app.main()
