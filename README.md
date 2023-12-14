# youtube_data
Youtube Data Warhousing And Harvesting

---

# YouTube Data Collector and Storage

This Python script grabs information from YouTube, like channel details and video stats, using the YouTube API. It then saves all this data in two places: a database called `Youtube` using MongoDB, and another one named `youtube_data` using PostgreSQL.

## What it Does

- **YouTube Info Fetcher:** It's like a detective for YouTube, getting facts about channels (names, subscribers, views) and details about videos (titles, tags, comments).
- **Data Safekeeping:** It saves this info in MongoDB and also creates tables in a PostgreSQL database to organize everything neatly.
- **Easy Access with Streamlit:** There's a simple way to look at all this data using Streamlit, a friendly interface where you can see and search through everything.

## How to Use

1. **Setting Up:** Make sure you have some specific programs and libraries installed.
2. **Get an API Key:** You'll need a special key to use the YouTube tools. Stick that key in the script where it says `Api_Id`.
3. **Prepare Databases:** Set up MongoDB and PostgreSQL databases and let the script create tables to store the YouTube info.
4. **Run the Script:** Fire up the Python script, and it'll start collecting and organizing data.
5. **Explore with Streamlit:** After running the script, visit the provided URL in your web browser to check out the data using Streamlit. It's easy and interactive!

## How the Code is Organized

Think of the script like a story with different parts:

- **YouTube Data Grabber:** Gets info from YouTube using their special tools.
- **Data Storer:** Keeps everything safe in MongoDB and PostgreSQL.
- **Streamlit Show-off:** Lets you easily see all the gathered info.
- **Data Queries:** It can search through the stored data to find interesting things.

## Things to Remember

- **Keep Secrets Safe:** Make sure to protect your special keys and passwords.
- **Play Nice with the API:** Don't ask for too much from YouTube at once!

## Join the Fun

Want to add something cool to the script? You can! Make your changes and join the team by sending in your ideas.

---

