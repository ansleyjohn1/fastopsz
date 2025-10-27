"""
Query Score Inference - Simple API for using the trained model
"""

import torch
from transformers import AutoTokenizer
from model_code import QueryScoreModel


class QueryScorePredictor:
    """
    Simple predictor for query-schema scoring.

    Usage:
        predictor = QueryScorePredictor('model/final_model.pt', 'model/')
        score = predictor.predict(query, schema, joins)

    Methods:
        predict(query, schema, joins) -> float
            Returns confidence score 0.0-1.0

        predict_batch(queries, schemas, joins_list) -> list
            Returns list of scores for multiple inputs

        is_answerable(query, schema, joins, threshold=0.5) -> bool
            Returns True if score > threshold
    """

    def __init__(self, model_path, tokenizer_path, device='cpu'):
        """
        Initialize the predictor.

        Args:
            model_path: Path to trained model (.pt file)
            tokenizer_path: Path to tokenizer directory
            device: 'cpu' or 'cuda' (default: 'cpu')

        Example:
            predictor = QueryScorePredictor(
                model_path='model/final_model.pt',
                tokenizer_path='model/'
            )
        """
        self.device = torch.device(device)
        self.max_length = 256

        # Load tokenizer
        print(f"Loading tokenizer from {tokenizer_path}...")
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)

        # Load model
        print(f"Loading model from {model_path}...")
        self.model = QueryScoreModel()
        checkpoint = torch.load(model_path, map_location=self.device)
        self.model.load_state_dict(checkpoint['model_state_dict'])
        self.model.to(self.device)
        self.model.eval()
        print("✓ Model loaded successfully!\n")

    def format_input(self, query, schema, joins):
        """
        Format query, schema, and joins into model input.

        Args:
            query: Natural language query string
            schema: Schema description (tables with columns)
            joins: Join conditions

        Returns:
            Formatted text string

        Example:
            text = predictor.format_input(
                query="Find all employees in Sales",
                schema="Table: employees (id, name, dept_id)\\nTable: departments (id, name)",
                joins="employees.dept_id = departments.id"
            )
        """
        return f"""Query: {query}
                   Schema: {schema}
                   Joins: {joins}
                   Score (0-1) how well these tables can answer the question:"""

    def predict(self, query, schema, joins):
        """
        Predict confidence score for query-schema pair.

        Args:
            query: Natural language query string
            schema: Schema description (tables with columns)
            joins: Join conditions

        Returns:
            float: Confidence score between 0.0 and 1.0
                - 0.8-1.0: High confidence (answerable)
                - 0.5-0.8: Medium confidence (manual review)
                - 0.0-0.5: Low confidence (not answerable)

        Example:
            score = predictor.predict(
                query="Find all employees in Sales department",
                schema="Table: employees (id, name, dept_id)\\nTable: departments (id, name)",
                joins="employees.dept_id = departments.id"
            )
            # Returns: 0.9280
        """
        # Format input
        text = self.format_input(query, schema, joins)

        # Tokenize
        encoding = self.tokenizer(
            text,
            max_length=self.max_length,
            truncation=True,
            padding='max_length',
            return_tensors='pt'
        )

        input_ids = encoding['input_ids'].to(self.device)
        attention_mask = encoding['attention_mask'].to(self.device)

        # Predict
        with torch.no_grad():
            score = self.model(input_ids, attention_mask)

        return score.item()

    def predict_batch(self, queries, schemas, joins_list):
        """
        Predict scores for multiple query-schema pairs.

        Args:
            queries: List of query strings
            schemas: List of schema descriptions
            joins_list: List of join conditions

        Returns:
            List of confidence scores (floats 0.0-1.0)

        Example:
            scores = predictor.predict_batch(
                queries=["Find employees", "Show orders", "List products"],
                schemas=[schema1, schema2, schema3],
                joins_list=[joins1, joins2, joins3]
            )
            # Returns: [0.9280, 0.0987, 0.7234]
        """
        scores = []
        for query, schema, joins in zip(queries, schemas, joins_list):
            score = self.predict(query, schema, joins)
            scores.append(score)
        return scores

    def is_answerable(self, query, schema, joins, threshold=0.5):
        """
        Check if query is answerable with given schema.

        Args:
            query: Natural language query string
            schema: Schema description
            joins: Join conditions
            threshold: Score threshold (default 0.5)
                - Use 0.5 for general use
                - Use 0.7 for conservative filtering
                - Use 0.3 for permissive filtering

        Returns:
            bool: True if score > threshold, False otherwise

        Example:
            answerable = predictor.is_answerable(
                query="Find employees in Sales",
                schema="Table: employees (id, name, dept_id)\\nTable: departments (id, name)",
                joins="employees.dept_id = departments.id",
                threshold=0.5
            )
            # Returns: True
        """
        score = self.predict(query, schema, joins)
        return score > threshold


# Command-line interface
if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Query Score Predictor')
    parser.add_argument('--model_path', default='model/final_model.pt', help='Path to model file')
    parser.add_argument('--tokenizer_path', default='model/', help='Path to tokenizer directory')
    parser.add_argument('--query', help='Query string')
    parser.add_argument('--schema', help='Schema description')
    parser.add_argument('--joins', help='Join conditions')

    args = parser.parse_args()

    # Initialize predictor
    predictor = QueryScorePredictor(
        model_path=args.model_path,
        tokenizer_path=args.tokenizer_path
    )

    # If arguments provided, run prediction
    if args.query and args.schema and args.joins:
        score = predictor.predict(args.query, args.schema, args.joins)
        print(f"\nQuery: {args.query}")
        print(f"Score: {score:.4f}")
        print(f"Answerable: {'Yes' if score > 0.5 else 'No'}")
    else:
        # Interactive mode
        print("=" * 80)
        print("Query Score Predictor - Interactive Mode")
        print("=" * 80)
        print("\nExample 1: Answerable query")
        print("-" * 80)

        query1 = "Find all employees in Sales department"
        schema1 = """Table: employees (employee_id, name, email, department_id, salary)
Table: departments (department_id, department_name, location)"""
        joins1 = "employees.department_id = departments.department_id"

        score1 = predictor.predict(query1, schema1, joins1)
        print(f"Query: {query1}")
        print(f"Score: {score1:.4f}")
        print(f"Answerable: {predictor.is_answerable(query1, schema1, joins1)}")

        print("\n" + "=" * 80)
        print("Example 2: Not answerable (missing join)")
        print("-" * 80)

        query2 = "Find all employees in Sales department"
        schema2 = """Table: employees (employee_id, name, email, salary)
Table: departments (department_id, department_name, location)"""
        joins2 = "(none)"

        score2 = predictor.predict(query2, schema2, joins2)
        print(f"Query: {query2}")
        print(f"Score: {score2:.4f}")
        print(f"Answerable: {predictor.is_answerable(query2, schema2, joins2)}")
        print()
