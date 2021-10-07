from prefect import task, Flow

@task
def hello_world():
    print("Hello World!")

with Flow("my_first_flow") as f:
    r = hello_world()

f.run()