from app.analytics import process_vibration_window


def main() -> None:
    for i in range(5):
        device_id = f"sensor_{i}"
        vibration = 10.5 + i
        accel_samples = [vibration]
        result = process_vibration_window.delay(device_id, accel_samples)
        print(f"Dispatched task {i}: id={result.id}")
        print(f"Task {i} {result.id} completed: {result.ready()}")


if __name__ == "__main__":
    main()
