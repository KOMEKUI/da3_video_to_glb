import torch
print("torch :", torch.__version__)
print("CUDA runtime :", torch.version.cuda)
print("GPU :", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "None")