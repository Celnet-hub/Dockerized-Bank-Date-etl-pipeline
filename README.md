Docker Assignment

Your manager at CDE was amazed at your first Linux and SQL Task. However, your manager
now wants you to port your ETL script into Python, and he needs to run it on any system without
worrying about the underlying operating system.

You have now been tasked with building a fully managed ELT pipeline that has an Extract with
Python, Load into a Staging Database and then Transform with DBT. For this task, you need to
have three Services(containers) running

One for the Python Extract and Loading
Second for the Postgres Instance
Third for DBT

-

You can choose to use Docker Compose to start all three services (figure out the dependencies)
or use a shell script to start everything up at once. Next, schedule your pipeline to run every
morning at 12:00 a.m. using a cron job (the containers).

Submission Materials
- A GitHub repository with a detailed Architectural Diagram and Readme (this should contain the project overview, what you did and how any random person can run what you built (reproducibility)).
Logging showing what is occurring inside each Extract, Loading and Transforming steps
A dashboard from the ingested data
A well-detailed medium and Linkedln post (summary that points to the medium article)

N.B. Ensure all your images are pushed to Docker Hub, so that anyone can run the images and
test them without rebuilding the images. This should be what you have in either your Docker
compose file or your shell script(if you plan to use the scripting route)

Submission Deadline
Submit your GitHub link, Medium Article Link and Linkedln Post Link in the Submission Form by
the 9th of October 2025.
