[YouTube Data Harvesting and Warehousing](https://github.com/gopinathvenkatesan01/Youtube-Data-harvesting)

<p align="center">
  <img src="https://github.com/gopinathvenkatesan01/Youtube-Data-harvesting/assets/162252390/dad37f56-88c1-4d1d-9515-87e566681932" alt="Project WorkFlow" width="690" height="390">
 </p>

**Introduction**

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

**Table of Contents**

1. Key Technologies and Skills
2. Installation
3. Usage
4. Features
5. Retrieving data from the YouTube API
6. Storing data in MongoDB
7. Migrating data to a SQL data warehouse
8. Data Analysis
9. Contributing
10. License
11. Contact

**Key Technologies and Skills**
- Python scripting
- Data Collection
- API integration
- Streamlit
- Data Management using MongoDB (Atlas) and SQL

**Installation**

To run this project, you need to install the following packages:
```python
pip install google-api-python-client
pip install pymongo
pip install pandas
pip install psycopg2
pip install streamlit
pip install streamlit_option_menu
pip install millify
```

**Usage**

To use this project, follow these steps:

1. Clone the repository: ```git clone https://github.com/gopinathvenkatesan01/Youtube-Data-harvesting.git```
2. Install the required packages: ```pip install -r requirements.txt```
3. Access the project folder ```cd main```
3. Run the Streamlit app: ```streamlit run main.py```
4. Access the app in your browser at ```http://localhost:8501```

**Features**

- Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
- Analyze and visualize data using Streamlit.
- Store the retrieved data in a MongoDB database.
- Migrate the data to a SQL data warehouse.
- Perform queries on the SQL data warehouse.
- Gain insights into channel performance, video metrics, and more.

**Retrieving data from the YouTube API**

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a JSON file.

**Storing data in MongoDB**

The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it can be overwritten with user consent. This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

**Migrating data to a SQL data warehouse**

The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

**Data Analysis**

The project provides comprehensive data analysis capabilities using Plotly and Streamlit. With the integrated Plotly library, users can create interactive and visually appealing charts and graphs to gain insights from the collected data.

- **Channel Analysis:** Channel analysis includes insights on  videos, subscribers, views, likes, comments.

- **Video Analysis:** Video analysis focuses on views, likes, comments for latest 50 videos and comments.


The Streamlit app provides an intuitive interface to interact with the charts and explore the data visually. Users can customize the visualizations, filter data, and zoom in or out to focus on specific aspects of the analysis.

**Contributing**

Contributions to this project are welcome! If you encounter any issues or have suggestions for improvements, please feel free to submit a pull request.

**License**

This project is licensed under the MIT License. Please review the LICENSE file for more details.

**Contact**

📧 Email: gopinathvenakatesan01@gmail.com

🌐 LinkedIn: [linkedin.com/in/gopinath-venkatesan-9707022a7](https://www.linkedin.com/in/gopinath-venkatesan-9707022a7/)

For any further questions or inquiries, feel free to reach out. We are happy to assist you with any queries.
