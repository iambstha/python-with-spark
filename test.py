import torch
import platform
import psutil
import cpuinfo


print("Is CUDA available? ", torch.cuda.is_available())
if torch.cuda.is_available():
    print("GPU Name:", torch.cuda.get_device_name(0))
    print("GPU Memory Allocated:", torch.cuda.memory_allocated(0) / 1024**2, "MB")
    print("GPU Memory Cached:", torch.cuda.memory_reserved(0) / 1024**2, "MB")

print("Number of CPU Cores:", torch.get_num_threads())


print("System:", platform.system(), platform.release())
print("Processor:", platform.processor())
print("CPU Cores:", psutil.cpu_count(logical=False))
print("Total RAM:", round(psutil.virtual_memory().total / (1024**3), 2), "GB")



info = cpuinfo.get_cpu_info()
print("CPU Brand:", info["brand_raw"])
print("Architecture:", info["arch"])
print("Bits:", info["bits"])
print("L2 Cache Size:", info["l2_cache_size"])

