

import faust

topic = app.topic("com.udacity.streams.purchases")

@app.agent(topic)
async def clickevent(clickevents):
    async for clickevent in clickevents:
        print(clickevent)


if __name__ == "__main__":
    app.main()
