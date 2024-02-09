import subprocess
import time
import multiprocessing
import csv

def run_instance(args):
    instance_id, shared_data, lock = args
    # Define the command to run your benchmark
    command = ["./b+tree.out", "file", "../../data/b+tree/mil.txt", "command", "../../data/b+tree/command.txt"]
    
    # Run the benchmark using subprocess
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Initialize variables for tracking GPU utilization time
    gpu_utilization_started = False
    gpu_utilization_start_time = None
    power_consumption_samples = []  # List to store power consumption samples
    
    # Monitor process while running
    while process.poll() is None:
        # Get GPU utilization using nvidia-smi
        gpu_utilization = get_gpu_utilization()
        
        # If GPU utilization is non-zero, start or update the utilization timer
        if gpu_utilization > 0:
            if not gpu_utilization_started:
                gpu_utilization_started = True
                gpu_utilization_start_time = time.time()
        else:
            if gpu_utilization_started:
                gpu_utilization_started = False
                # Calculate and update the total GPU utilization time
                with lock:
                    shared_data['gpu_utilization_time'] += time.time() - gpu_utilization_start_time
        
        # Get power consumption using nvidia-smi
        power_consumption = get_power_consumption()
        
        # Append power consumption to the list of samples and update shared data
        power_consumption_samples.append(power_consumption)
        with lock:
            shared_data['gpu_utilization'] = max(shared_data['gpu_utilization'], gpu_utilization)
        # Adjust sleep time according to your monitoring frequency
        time.sleep(0.001)  # Adjust this as needed
    
    # After the process finishes, write power consumption samples to CSV file
    with open(f"power_consumption_instance_{instance_id}.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Time (s)", "Power Consumption (W)"])
        for t, power_consumption in power_consumption_samples:
            writer.writerow([t, power_consumption])  

def get_gpu_utilization():
    # Use nvidia-smi to get GPU utilization
    command = ["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"]
    result = subprocess.run(command, stdout=subprocess.PIPE)
    output_lines = result.stdout.decode().strip().split('\n')
    # The output contains utilization of each GPU, we'll just take the first one for simplicity
    gpu_utilization = int(output_lines[0])
    return gpu_utilization

def get_power_consumption():
    # Use nvidia-smi to get power consumption
    command = ["nvidia-smi", "--query-gpu=power.draw", "--format=csv,noheader,nounits"]
    result = subprocess.run(command, stdout=subprocess.PIPE)
    output_lines = result.stdout.decode().strip().split('\n')
    # The output contains power consumption of each GPU, we'll just take the first one for simplicity
    power_consumption = float(output_lines[0])
    return (time.time(),power_consumption)

if __name__ == "__main__":
    num_instances = 50  # Change this to the desired number of instances

    # Create a shared dictionary for collecting GPU utilization, GPU utilization time, and power consumption samples
    manager = multiprocessing.Manager()
    shared_data = manager.dict({'gpu_utilization': 0, 'gpu_utilization_time': 0})
    lock = manager.Lock()

    # Run instances in parallel
    with multiprocessing.Pool(processes=num_instances) as pool:
        pool.map(run_instance, [(i, shared_data, lock) for i in range(num_instances)])

    # Extract data from shared memory
    max_gpu_utilization = shared_data['gpu_utilization']
    total_gpu_utilization_time = shared_data['gpu_utilization_time']

    print("Maximum GPU Utilization:", max_gpu_utilization)
    print("Total GPU Utilization Time (s):", total_gpu_utilization_time)
    # Write data to CSV file
    with open("gpu_data.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Maximum GPU Utilization (%)","Total GPU Utilization Time (s)"])
        writer.writerow([max_gpu_utilization, total_gpu_utilization_time])