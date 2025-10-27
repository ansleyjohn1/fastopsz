from schema_inspector import UniversalSchemaInspector
from inference_api import QueryScorePredictor

class GraphRanker:
    def __init__(self, question, connections):
        self.question = question
        self.connections = connections
        self.schema_metadata = self.get_schema_metadata()
        self.predictor = QueryScorePredictor(
        model_path='model/final_model.pt',
        tokenizer_path='model/'
    )

    def get_schema_metadata(self):
        
        schema_metadata = {}
        for conn in self.connections:
            inspector = UniversalSchemaInspector(conn)
            inspector.connect()

            # Get all tables
            tables = inspector.get_all_tables()
            for table_name in tables:
                try:
                    # Get schema
                    schema = inspector.get_table_schema(table_name)
                    schema_metadata[table_name] = schema
                except Exception as e:
                    print(f"Error retrieving schema for table {table_name} in connection {conn['connection_id']}: {e}")
        return schema_metadata

    def graph_ranker(self, combinations, historical_data):
   

        # Job 1: Fast heuristic filter
        # NO model calls - just math + keyword matching
        top_10 = self.heuristic_filter(combinations, self.question, self.schema_metadata, historical_data)
        # Time: ~50ms

        # Job 2: Model scoring
        # 10 model calls - score each combo, return all with scores
        scored_top_10 = self.rank_with_model(top_10, self.question, self.schema_metadata)
        # Time: ~200ms
        # Returns: [{score: 0.95, complexity: 2, combination: {...}}, ...]

        return scored_top_10
    
    def heuristic_filter(self, combinations, question, schema_metadata, historical_data):
        """
        Filter 30 combinations to top 10 using heuristics.
        NO model calls - just math and keyword matching.

        Input: 30 combinations from Joinability Sheriff
        Output: Top 10 combinations
        Time: <50ms
        """
        scored = []

        for combo in combinations:
            score = self.calculate_heuristic_score(combo, question, schema_metadata, historical_data)
            scored.append((combo, score))

        # Sort by score, return top 10
        scored.sort(key=lambda x: x[1], reverse=True)
        return [combo for combo, score in scored[:10]]


    def calculate_heuristic_score(self, combo, question, schema_metadata, historical_data):
        """
        4-factor scoring formula.
        All factors use existing data - no model inference.
        """
        score = 0.0

        # Factor 1: Simplicity (30% weight)
        # From Joinability Sheriff output: combo["complexity"]
        if combo["complexity"] == 1:
            simplicity = 1.0  # Single table
        elif combo["complexity"] == 2:
            simplicity = 0.8  # Direct pair
        else:
            simplicity = 0.6  # Chain (3 tables)

        score += 0.30 * simplicity

        # Factor 2: Join Quality (20% weight)
        # From Joinability Sheriff output: combo["join_paths"]
        num_joins = len(combo["join_paths"])

        if num_joins == 0:
            join_quality = 1.0  # Single table, no joins
        elif num_joins == 1:
            join_quality = 0.9  # One direct join
        else:
            join_quality = 0.7  # Multiple joins (chain)

        score += 0.20 * join_quality

        # Factor 3: Column Coverage (40% weight) ⬅️ LIGHTWEIGHT SEMANTIC CHECK
        # Check if combo has columns matching question keywords
        coverage = self.calculate_column_coverage(combo, question, schema_metadata)
        score += 0.40 * coverage

        # Factor 4: Historical Success (10% weight)
        # From historical logs: has this combo worked before?
      #  historical_score = self.get_historical_success(combo, historical_data)
       # score += 0.10 * historical_score

        return score


    def calculate_column_coverage(self, combo, question, schema_metadata):
        """
        Lightweight keyword matching (NOT full semantic analysis).

        Check: Does this combo have columns matching question keywords?
        NO model call - just string matching.
        """
        # Step 1: Extract keywords from question (simple, no model)
        keywords = self.extract_keywords_simple(question)
        # Example: "active and inactive accounts" → ["active", "inactive", "accounts"]

        # Step 2: Get all column names from this combo
        combo_columns = []
        for table_name in combo["tables"]:
            table_schema = schema_metadata[table_name]
            for col in table_schema["columns"]:
                combo_columns.append(col["name"].lower())

        # Step 3: Check keyword overlap with column names
        matches = 0
        for keyword in keywords:
            for col_name in combo_columns:
                # Fuzzy match: "status" matches "account_status"
                if keyword in col_name or col_name in keyword:
                    matches += 1
                    break

        # Calculate coverage
        if len(keywords) == 0:
            return 0.5  # Neutral if no keywords

        coverage = matches / len(keywords)
        return coverage


    def extract_keywords_simple(self, question):
        """
        Extract keywords WITHOUT model call.
        Simple approach: remove stop words, keep nouns/adjectives.
        """
        import re

        # Simple stop words list
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
            'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
            'should', 'could', 'may', 'might', 'must', 'can', 'get', 'show', 'list'
        }

        # Extract words
        words = re.findall(r'\w+', question.lower())

        # Filter: remove stop words, keep words > 3 chars
        keywords = [w for w in words if w not in stop_words and len(w) > 3]

        return keywords


    def get_historical_success(self, combo, historical_data):
        """
        Check if this combo has worked before.
        From logs database - no model call.
        """
        # Create combo key (sorted table names)
        combo_key = tuple(sorted(combo["tables"]))

        # Look up in historical data
        if combo_key in historical_data:
            return historical_data[combo_key]["success_rate"]
        else:
            return 0.5  # Neutral for unknown combos
        
    def rank_with_model(self, top_10_combos, question, schema_metadata):
        """
        Score each combination individually using tiny model.

        Input: 10 combinations from Job 1
        Output: All 10 combinations with their scores (sorted by score)
        Model calls: 10 (one per combo)
        Time: ~200ms total (~20ms per call with 5MB model)
        """
        scored_combos = []

        # Score each combination individually
        for combo in top_10_combos:
            # Build prompt for this specific combo
            #prompt = self.build_scoring_prompt(question, combo, schema_metadata)

            tables = combo["tables"]
            join_paths = combo.get("join_paths", [])
            
            # Build schema string
            schema_parts = []
            for table_name in tables:
                if table_name in schema_metadata:
                    table_meta = schema_metadata[table_name]
                    columns = table_meta.get("columns", [])
                    
                    # Extract column names (handle if columns are dicts or strings)
                    if columns and isinstance(columns[0], dict):
                        column_names = [col.get("name", col.get("column_name", "")) 
                                    for col in columns]
                    else:
                        column_names = columns
                    
                    # Format: "Table: table_name (col1, col2, col3)"
                    columns_str = ", ".join(column_names)
                    schema_parts.append(f"Table: {table_name} ({columns_str})")
            
            schema = "\\n".join(schema_parts)
            
            # Build joins string
            joins_parts = []
            for join_path in join_paths:
                from_table = join_path.get("from_table", "")
                to_table = join_path.get("to_table", "")
                from_columns = join_path.get("from_columns", [])
                to_columns = join_path.get("to_columns", [])
                
                # Handle multiple column joins
                for from_col, to_col in zip(from_columns, to_columns):
                    joins_parts.append(f"{from_table}.{from_col} = {to_table}.{to_col}")
            
            joins = "\\n".join(joins_parts) if joins_parts else ""

            # Call tiny model (5MB, very fast)
            score = self.predictor.predict(question,schema,joins)

            # Model returns float between 0-1
            score = float(score.strip())

            # Enrich with full metadata
            enriched = self.enrich_with_metadata(combo, schema_metadata)

            scored_combos.append({
                "combination": enriched,
                "score": score,
                "complexity": combo["complexity"]
            })

        # Sort by score descending
        scored_combos.sort(key=lambda x: x["score"], reverse=True)

        # Return all 10 with scores
        return scored_combos


    def enrich_with_metadata(self,combo, schema_metadata):
        """
        Add full schema metadata to combination.
        """
        enriched = {
            "tables": combo["tables"],
            "join_paths": combo["join_paths"],
            "complexity": combo["complexity"],
            "metadata": {}
        }

        # Add full schema for each table
        for table_name in combo["tables"]:
            table_schema = schema_metadata[table_name]

            enriched["metadata"][table_name] = {
                "columns": table_schema["columns"],  # All columns with types
                "primary_key": table_schema.get("primary_key", []),
                "foreign_keys": table_schema.get("foreign_keys", []),
                "indexes": table_schema.get("indexes", []),
                "row_count": table_schema.get("estimated_rows", 0)
            }

        return enriched


    def build_scoring_prompt(self, question, combo, schema_metadata):
        """
        Build prompt for scoring ONE combination.
        Simple format: Question + Tables + Columns
        """
        prompt = f"""Question: {question}

    Tables: {', '.join(combo['tables'])}

    Columns:
    """

        # Show columns from each table
        for table_name in combo["tables"]:
            table_schema = schema_metadata[table_name]
            col_list = [col["name"] for col in table_schema["columns"][:15]]  # First 15 columns
            prompt += f"{table_name}: {', '.join(col_list)}\n"

        # Show joins if any
        if combo["join_paths"]:
            prompt += "\nJoins:\n"
            for jp in combo["join_paths"]:
                prompt += f"{jp['from_table']}.{jp['from_columns'][0]} = {jp['to_table']}.{jp['to_columns'][0]}\n"

        prompt += "\nScore (0-1) how well these tables can answer the question:"

        return prompt