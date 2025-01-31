# spark-etl-tool
## PREREQUISITES
1. Clone the repo. SSH preferred, instructions here: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent
2. Install python3 (latest): https://www.geeksforgeeks.org/download-and-install-python-3-latest-version/
3. In project root, run: ```python3 -m venv .venv```
4. In project root (Linux only), run: ```source .venv/bin/activate```
5. In project root run: ```pip install -r requirements.txt```
6. Install java 17 jdk: https://www.oracle.com/java/technologies/javase/jdk17-0-13-later-archive-downloads.html
7. test if java is ready: ```java --version```
8. If java is not ready, set PATH variable to include the java bin (wherever located) and set the JAVA_HOME variable to the JDK installation folder: https://www.geeksforgeeks.org/how-to-set-java-path-in-windows-and-linux/
9. In project code directory, make sure there is an "output" folder.
10. Create .env.sh file in code directory with currently these secret values:

    ```export MARKETSTACK_API_KEY="<secret value>"
    export POSTGRES_JDBC_URL="<secret value>"
    export POSTGRES_USER="<secret value>"
    export POSTGRES_PASSWORD="<secret value>"```
Note: replace "secret value" with corresponding secret.

11. In code directory, run: ```source .env.sh```
   
## STARTUP
1. In project root directory, make sure prerequisites have been done, the virtual environment is activated and all required packages are installed.
2. In code directory, run a sample test to bring in 100000 ct scan images: ```python Worker.py ctscans```


## TOOLS
1. If on Windows, I recommend using WSL. How to enable here: https://learn.microsoft.com/en-us/windows/wsl/install
2. I recommend using vscode for development: https://code.visualstudio.com/download
3. You can setup vscode to use WSL by default: https://code.visualstudio.com/docs/remote/wsl

## VSCode Extensions
1. SQL query client: https://marketplace.visualstudio.com/items?itemName=cweijan.vscode-database-client2
2. WSL: https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-wsl

## NEXT STEPS
1. ~~Test JDBC connections to local or hosted relational DBs~~
2. Create custom py script for loading into non-relational DBs
3. Email/text alerts for job kickoff, failures, warnings, and completion. This configuration will be set in the JSON config. Notes on this: Allow different levels of notifications (only on completion, only on failure, etc.). Also will send standard output from the job run in the email/text. 
4. UI for scheduling jobs, creating jobs, maintaining jobs (Personal preference is React, will make this in a separate repo, and we will need a relational DB for basic metadata stores.)
5. Use Deepseek (or alternatives) to generate ML code based on user input (updating UI in step 4 to work with this as well), with data ingestion setup using this app. (Again, different standalone repo)

## Notes on Deepseek or alternative:
1. User creates automated pipeline, storing data where they please.
2. User then provides some input on what the goal of the ML model is. Some structured input, some can be freeform for the AI to decide.
3. User then starts the job, process automates feature selection (for supervised, structured learning to start), process automates parameter tuning and creates the code (in language of choice) and the user can do with it as they please.
4. After user confirms code is good to go, UI allows the user to run it in a jupyter notebook where they can then download the notebook, run it locally or run it directly in the UI.
