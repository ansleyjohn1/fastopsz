"""
Query Score Model - TinyBERT Implementation
Outputs 0-1 confidence scores for query-schema matching
"""

import torch
import torch.nn as nn
from transformers import AutoTokenizer, AutoModel


class QueryScoreModel(nn.Module):
    """
    Tiny model that outputs 0-1 scores for query-schema matching.

    Architecture:
    - Base: TinyBERT (4.4M parameters)
    - Hidden size: 128
    - Head: Linear(128→32) → ReLU → Dropout → Linear(32→1) → Sigmoid

    Input:
    - Query: Natural language question
    - Schema: Database tables with columns
    - Joins: Join conditions between tables

    Output:
    - Score: Float between 0.0 and 1.0
      - 0.8-1.0: Query is answerable (high confidence)
      - 0.5-0.8: Uncertain (manual review recommended)
      - 0.0-0.5: Query is NOT answerable (low confidence)
    """

    def __init__(self, model_name='prajjwal1/bert-tiny'):
        """
        Initialize the model.

        Args:
            model_name: HuggingFace model name (default: prajjwal1/bert-tiny)
        """
        super().__init__()
        self.encoder = AutoModel.from_pretrained(model_name)
        hidden_size = self.encoder.config.hidden_size  # 128 for bert-tiny

        # Regression head to output 0-1 score
        self.regressor = nn.Sequential(
            nn.Linear(hidden_size, 32),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(32, 1),
            nn.Sigmoid()  # Output between 0 and 1
        )

    def forward(self, input_ids, attention_mask):
        """
        Forward pass.

        Args:
            input_ids: Token IDs [batch_size, seq_len]
            attention_mask: Attention mask [batch_size, seq_len]

        Returns:
            scores: Confidence scores [batch_size]
        """
        # Get encoder output
        outputs = self.encoder(input_ids=input_ids, attention_mask=attention_mask)

        # Use [CLS] token (first token)
        cls_embedding = outputs.last_hidden_state[:, 0, :]

        # Get score
        score = self.regressor(cls_embedding)
        return score.squeeze(-1)  # Remove last dimension
