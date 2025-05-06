import torch
from transformers import AutoTokenizer

class Batch():
    def __init__(self, seq_ids, domain_ids, attention_mask):
        self.seq_ids = seq_ids
        self.domain_ids = domain_ids
        self.attention_mask = attention_mask
           
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

esm_tokenizer = AutoTokenizer.from_pretrained(f"facebook/esm1b_t33_650M_UR50S")
def esm_encode_sequence(sequence):
    return esm_tokenizer(
        [e['sequence'] for e in b],
        return_tensors="pt", 
        padding='max_length', 
        truncation=True, 
        max_length=max_seq_len
    )

def chop_indices(chopping, max_seq_len):
    domains = {}
    # split on '*' to allow multiple domains, and '_' for discontinuous bits
    for i, domain in enumerate(chopping.split("*")):
        segments = []
        for part in domain.split('_'):
            start_str, stop_str = part.split('-')
            start, stop = int(start_str), int(stop_str)
            # convert to 0-based, exclusive-stop indexing
            segments.append((start - 1, stop))
        domains[i+1] = segments

    output = [0] * max_seq_len
    for k, ranges in domains.items():
        for start, stop in ranges:
            end   = min(stop, max_seq_len)
            if start >= end:
                continue
            length = end - start
            # fill that slice with k
            output[start:end] = [k] * length

    return output

def data_to_tensor_batch(b, max_seq_len):
    seq_inputs = torch.LongTensor([encode_sequence(e['Sequence'], max_seq_len) for e in b if e['Sequence'] is not None])
    domain_inputs = torch.LongTensor([chop_indices(e['chopping_star'], max_seq_len) for e in b if e['Sequence'] is not None])
    return Batch(seq_inputs, domain_inputs)
