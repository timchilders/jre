# JRE Political Sentiment Analysis

This project aims to analyze the evolution of Joe Rogan's political leanings through sentiment analysis of The Joe Rogan Experience (JRE) podcast transcripts over the past decade (2013-2023).

## Project Goals

1. Collect and process JRE podcast transcripts, focusing on segments discussing:
   - Political topics
   - Cultural issues
   - Social commentary
   - Policy discussions
   - Political figures and movements

2. Perform sentiment analysis to:
   - Track changes in political stance over time
   - Identify shifts in opinion on specific topics
   - Analyze emotional content when discussing different political ideologies
   - Measure changes in language and rhetoric around political subjects

3. Create visualizations and analysis showing:
   - Temporal trends in political sentiment
   - Topic modeling of political discussions
   - Key turning points in political positioning
   - Correlation between guests' political leanings and expressed sentiments

## Technical Implementation

### Data Collection
- Source transcripts from available APIs and databases
- Implement selective extraction of political/cultural segments
- Create a structured database of relevant transcript segments

### Analysis Pipeline
1. Text preprocessing and cleaning
2. Political topic identification and extraction
3. Sentiment analysis using NLP models
4. Temporal trend analysis
5. Topic modeling and clustering

### Technologies
- Python for data processing and analysis
- Natural Language Processing (NLP) libraries
- Machine Learning frameworks for sentiment analysis
- Data visualization tools

## Data Sources

Potential sources for JRE transcripts:
1. YouTube auto-generated transcripts
2. Fan-maintained transcript databases
3. Official podcast transcription services
4. Third-party APIs

## Project Status

Currently in active development:

### Completed
1. Initial project setup and structure
2. Identified primary data source: JRE Clips YouTube channel
3. Developed transcript collection script with:
   - YouTube API integration
   - Rate limiting and error handling
   - Automatic transcript fetching
   - JSON storage format

### In Progress
1. Collecting transcripts from JRE Clips channel (2017-2025)
2. Building dataset of politically-relevant segments

### Next Steps
1. Develop preprocessing pipeline to:
   - Clean and normalize transcripts
   - Filter for political content
   - Extract relevant metadata (dates, topics, guests)
2. Create initial sentiment analysis pipeline
3. Implement topic modeling to identify political discussions

## Contributing

This project is open for contributions. Please read the contributing guidelines before submitting pull requests.

## License

[License details to be determined]