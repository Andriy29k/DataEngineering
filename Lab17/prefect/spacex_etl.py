from prefect import flow, task
from prefect_shell import ShellOperation

# Завдання 1: Збирання даних та надсилання в Kafka
@task
def collect_and_send_data():
    ShellOperation(
        commands=["python /opt/prefect/flows/collect_data.py"],
        stream_output=True
    ).run()

# Завдання 2: Збереження даних у Bronze таблиці
@task
def save_to_bronze():
    ShellOperation(
        commands=["python /opt/prefect/flows/save_to_bronze.py"],
        stream_output=True
    ).run()

# Завдання 3: Обробка даних та збереження у Silver таблиці
@task
def save_to_silver():
    ShellOperation(
        commands=["python /opt/prefect/flows/save_to_silver.py"],
        stream_output=True
    ).run()

# Завдання 4: Агрегація даних та збереження у Gold таблиці
@task
def save_to_gold():
    ShellOperation(
        commands=["python /opt/prefect/flows/save_to_gold.py"],
        stream_output=True
    ).run()

# Flow, який оркеструє всі задачі
@flow
def spacex_flow():
    # Виконання всіх завдань по черзі
    data = collect_and_send_data.submit()
    bronze = save_to_bronze.submit(wait_for=[data])
    silver = save_to_silver.submit(wait_for=[bronze])
    save_to_gold.submit(wait_for=[silver])

if __name__ == "__main__":
    spacex_flow()
