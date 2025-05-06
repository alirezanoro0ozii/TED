import torch
from transformers import AutoTokenizer

class Batch():
    def __init__(self, seq_ids):
        self.seq_ids = seq_ids
           
    def gpu(self, device):      
        for name, var in self.__dict__.items():
            if isinstance(var, torch.Tensor) or isinstance(var, torch.LongTensor):
                setattr(self, name, var.to(device))
            else:
                setattr(self, name, {k: v.to(device) for k, v in var.items()})

# Character vocabulary for protein sequences (20 amino acids + 1 padding)
vocab = "ACDEFGHIKLMNPQRSTVWY"
char_to_idx = {char: idx + 1 for idx, char in enumerate(vocab)}  # Start index from 1 for padding
# Sequence encoder: Convert the protein sequence into integers
def encode_sequence(sequence, max_seq_len):
    if len(sequence) > max_seq_len:
        return [char_to_idx.get(char, 0) for char in sequence]
    else:
        return [char_to_idx.get(char, 0) for char in sequence] + [0 for _ in range(max_seq_len - len(sequence))]  # 0 for unknown characters or padding 

def data_to_tensor_batch(b, max_seq_len):
    inputs = torch.LongTensor([encode_sequence(e['Sequence'], max_seq_len) for e in b])
    return Batch(inputs)

#################################### 11_ESM_FineTune ####################################
esm_tokenizer = AutoTokenizer.from_pretrained(f"facebook/esm1b_t33_650M_UR50S")
def esm_data_to_tensor_batch(b, max_seq_len):
    inputs = esm_tokenizer(
        [e['sequence'] for e in b],
        return_tensors="pt", 
        padding='max_length', 
        truncation=True, 
        max_length=max_seq_len
    )
    return Batch(inputs)
