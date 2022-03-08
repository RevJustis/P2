# P2

   
# Road Safety Analysis
## Project Description
Using data from the National Highway Traffic Aafety Administration, this project examined trends in vehicle-related injuries and fatalities. It looks at accidents involving motorized vehicles and their occupants as well as those affecting pedestrians and cyclists. Queries included the following:

- Big Picture: Graph the trend of fatalities in entire USA, then graph the trend of fatalities in individual states. What are the differences, do they all have the same basic movement upward and downwards? Which states are safest (could be cross referenced with states with high public transport use to find correlation)?
- Rural vs Urban: Combine rural state data together and urban state data together, compare graphed trends (specifically death rates of vehicle types, and overall death rates across time).
Cyclists Urban and Rural trends in vehicular-related incidents involving pedestrians. Do certain urban and/or rural areas present surprising trends?
- Pedestrians: What are the trends across the age continuum that affect the injury and fatality rate of pedestrians, nationwide and within individual states? What are the sex/gender trends? Are they the same, do the trends have any similarities?
- Vehicle: Find which vehicles are most dangerous to be around (those that result in fatalities) vs most dangerous to be inside.


## Technologies Used
- IntelliJ/Emacs
- Scala ver. 2.11.12
- Spark-Core ver. 2.3.1
- Magit (administrative git client)
- SBT ver. 1.5.7+
- Zeppelin ver. 0.10.0
- Asana

## Features

- User Interface
- Admin feature for managing UI
- Interactive Menu
- User Account Creation

To Do List:
- AWS Integration
- Package project with Spark Submit
- Create More Complex Queries

## Getting Started
- Set up central project repository in Github, which included creating separate branches in the repository for individual queries, development, main, optimization, User Interface, and Userpass (where user account information stored)
- Clone central project repository to each participant's Intellij
```
git clone https://github.com/revjustis/p2.git
```
- Find data for project
- Ingest data from National Highway Traffic Safety Administration in the form of CSV files
- Download Zeppelin for presentation of project and launch in Intellij

## Usage

When users run the project, they will initially be prompted to create an account (or if they have one already to log in). Then, a menu of options appears that allows users to choose between topics related to our query categories above. Within each of these topics, users will be able to choose individual queries and see results of those queries.



## Collaborators
Developed by Jessica(Optimization Lead), Jonathan (Visualization Lead), Justis (GitHub Admin/Owner, Team Lead), Patrick(Co-Admin/Lead).
