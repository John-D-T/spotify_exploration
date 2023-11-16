# Music Exploration Project
The 'spotify_exploration' project's purpose is to explore musical data. Here we primarily use the Spotify API to aid with analysis, and use the 'polars' package to process and transform our data.

The structure of the pipeline follows ETL:

*Extract*: Obtain the relevant data using the Spotify API and other sources. The goal is to obtain data on popular songs, and song lyrics
    
   - Credits to https://towardsdatascience.com/extracting-song-data-from-the-spotify-api-using-python-b1e79388d50 for providing information on how to do this

*Transform*: Add necessary watermark columns, derived columns, and perform helpful joins

*Load*: Load the data to either a final .csv or .SQL Table


Ideas with the data include:

- Exploring the popularity of genres over time
- Digging into defining characteristics of songs by decade
- Generating a module which can parse hip-hop song lyrics and decipher rhyme schemes. 
   - This may require auxiliary rhyme dictionaries, or even potentially a personally generated dictionary.


Note: This project would not be possible with the help of Spotify and their API, accessible using 'Spotify for Developers':
https://developer.spotify.com/dashboard & https://developer.spotify.com/documentation/web-api/tutorials/getting-started


**The Project is using Python Version 3.11, details of the packages can be found in the requirements.txt**


