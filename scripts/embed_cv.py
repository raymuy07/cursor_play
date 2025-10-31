"""
CV Embedding Script
Extracts text from CV files (PDF or DOCX) and generates embeddings using sentence-transformers.
"""

import os
import sys
import pickle
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils import load_config, setup_logging

# Third-party imports for document processing
try:
    import PyPDF2
except ImportError:
    PyPDF2 = None

try:
    from docx import Document
except ImportError:
    Document = None

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
except ImportError:
    SentenceTransformer = None
    np = None


class TextEmbedder:
    """Shared base class for generating text embeddings using sentence-transformers."""
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", logger: Optional[logging.Logger] = None):
        """
        Initialize TextEmbedder with a specific model.
        
        Args:
            model_name: Name of the sentence-transformer model to use
            logger: Optional logger instance
        """
        if SentenceTransformer is None:
            raise ImportError(
                "sentence-transformers is required for embedding generation. "
                "Install it with: pip install sentence-transformers"
            )
        
        self.model_name = model_name
        self.logger = logger or setup_logging()
        self.logger.info(f"Loading sentence-transformer model: {model_name}...")
        
        try:
            self.model = SentenceTransformer(model_name)
            self.logger.info(
                f"Model loaded successfully. Embedding dimension: {self.model.get_sentence_embedding_dimension()}"
            )
        except Exception as e:
            raise RuntimeError(f"Error loading model {model_name}: {str(e)}")
    
    def embed_text(self, text: str) -> np.ndarray:
        """
        Generate embedding for the given text.
        
        Args:
            text: Text to embed
            
        Returns:
            1D numpy array containing the embedding vector
        """
        if not text or not text.strip():
            raise ValueError("Cannot generate embeddings for empty text")
        
        self.logger.debug(f"Generating embedding for text ({len(text)} characters)...")
        
        try:
            # Generate embedding (returns 1D numpy array)
            embedding = self.model.encode(text, convert_to_numpy=True)
            
            self.logger.debug(f"Embedding generated successfully. Dimension: {len(embedding)}")
            
            return embedding
            
        except Exception as e:
            raise RuntimeError(f"Error generating embedding: {str(e)}")


class CVReader:
    """Reads and extracts text from CV files (PDF or DOCX)."""
    
    def __init__(self, file_path: str, logger: Optional[logging.Logger] = None):
        """
        Initialize CVReader with a file path.
        
        Args:
            file_path: Path to the CV file
        """
        self.file_path = file_path
        self.file_extension = Path(file_path).suffix.lower()
        self.logger = logger or setup_logging()
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CV file not found: {file_path}")
        
        # Validate file extension
        if self.file_extension not in ['.pdf', '.docx']:
            raise ValueError(f"Unsupported file format: {self.file_extension}. Only .pdf and .docx are supported.")
    
    def extract_text(self) -> str:
        """
        Extract text from the CV file based on its extension.
        
        Returns:
            Extracted and cleaned text from the CV
        """
        if self.file_extension == '.pdf':
            return self._extract_from_pdf()
        elif self.file_extension == '.docx':
            return self._extract_from_docx()
        else:
            raise ValueError(f"Unsupported file format: {self.file_extension}")
    
    def _extract_from_pdf(self) -> str:
        """
        Extract text from a PDF file.
        
        Returns:
            Extracted text from all pages
        """
        if PyPDF2 is None:
            raise ImportError("PyPDF2 is required for PDF processing. Install it with: pip install PyPDF2")
        
        text_parts = []
        
        try:
            with open(self.file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                num_pages = len(pdf_reader.pages)
                
                self.logger.info(f"Reading PDF with {num_pages} pages...")
                
                for page_num in range(num_pages):
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    if text:
                        text_parts.append(text)
                
            full_text = "\n".join(text_parts)
            return self._clean_text(full_text)
            
        except Exception as e:
            raise RuntimeError(f"Error reading PDF file: {str(e)}")
    
    def _extract_from_docx(self) -> str:
        """
        Extract text from a DOCX file.
        
        Returns:
            Extracted text from all paragraphs
        """
        if Document is None:
            raise ImportError("python-docx is required for DOCX processing. Install it with: pip install python-docx")
        
        try:
            doc = Document(self.file_path)
            
            # Extract text from all paragraphs
            paragraphs = [paragraph.text for paragraph in doc.paragraphs if paragraph.text.strip()]
            
            # Also extract text from tables if any
            table_text = []
            for table in doc.tables:
                for row in table.rows:
                    row_text = []
                    for cell in row.cells:
                        if cell.text.strip():
                            row_text.append(cell.text.strip())
                    if row_text:
                        table_text.append(" | ".join(row_text))
            
            # Combine paragraphs and table text
            all_text = paragraphs + table_text
            full_text = "\n".join(all_text)
            
            self.logger.info(f"Read DOCX with {len(paragraphs)} paragraphs and {len(doc.tables)} tables")
            
            return self._clean_text(full_text)
            
        except Exception as e:
            raise RuntimeError(f"Error reading DOCX file: {str(e)}")
    
    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize extracted text.
        
        Args:
            text: Raw extracted text
            
        Returns:
            Cleaned text
        """
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove multiple newlines
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Strip leading/trailing whitespace
        text = text.strip()
        
        return text


class CVEmbedder:
    """Generates embeddings from CV text using sentence-transformers."""
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", logger: Optional[logging.Logger] = None):
        """
        Initialize CVEmbedder with a specific model.
        
        Args:
            model_name: Name of the sentence-transformer model to use
            logger: Optional logger instance
        """
        self.embedder = TextEmbedder(model_name=model_name, logger=logger)
        self.model_name = model_name
        self.logger = logger or setup_logging()
    
    def embed_text(self, text: str) -> Dict:
        """
        Generate embedding for the given text with metadata.
        
        Args:
            text: Text to embed
            
        Returns:
            Dictionary containing embedding and metadata
        """
        if not text or not text.strip():
            raise ValueError("Cannot generate embeddings for empty text")
        
        self.logger.info(f"Generating embedding for text ({len(text)} characters)...")
        
        try:
            # Generate embedding using shared TextEmbedder
            embedding = self.embedder.embed_text(text)
            
            # Create result dictionary with metadata
            result = {
                'embedding': embedding,
                'text': text,
                'model_name': self.model_name,
                'timestamp': datetime.now().isoformat(),
                'embedding_dim': len(embedding),
                'text_length': len(text)
            }
            
            self.logger.info(f"Embedding generated successfully. Dimension: {len(embedding)}")
            
            return result
            
        except Exception as e:
            raise RuntimeError(f"Error generating embedding: {str(e)}")
    
    def save_embedding(self, embedding_data: Dict, output_path: str):
        """
        Save embedding data to a pickle file.
        
        Args:
            embedding_data: Dictionary containing embedding and metadata
            output_path: Path where to save the pickle file
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save to pickle file
            with open(output_path, 'wb') as f:
                pickle.dump(embedding_data, f)
            
            self.logger.info(f"Embedding saved successfully to: {output_path}")
            
            # Print summary
            file_size = os.path.getsize(output_path) / 1024  # KB
            self.logger.info(f"File size: {file_size:.2f} KB")
            
        except Exception as e:
            raise RuntimeError(f"Error saving embedding: {str(e)}")


def main():
    """Main function to process CV and generate embeddings."""
    
    # Load configuration
    try:
        logger = setup_logging()
        config = load_config()
        cv_config = config.get('cv_embedding', {})
    except Exception as e:
        print(f"Error loading configuration")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("CV EMBEDDING GENERATION")
    logger.info("=" * 60)
    
    # Get configuration values from config file
    cv_path = cv_config.get('cv_file_path')
    output_path = cv_config.get('embeddings_output', 'data/cv_embeddings.pkl')
    model_name = cv_config.get('model_name', 'all-MiniLM-L6-v2')
    
    # Validate CV path
    if not cv_path:
        logger.error("Please specify a valid CV file path in config.yaml under cv_embedding.cv_file_path")
        sys.exit(1)
    
    try:
        # Step 1: Read CV file
        logger.info(f"Step 1: Reading CV from: {cv_path}")
        try:
            cv_reader = CVReader(cv_path, logger=logger)
            cv_text = cv_reader.extract_text()
        except Exception as e:
            logger.error(f"Error reading CV file: {e}")
            sys.exit(1)
        
        logger.info(f"Successfully extracted {len(cv_text)} characters from CV")
        logger.info(f"Preview: {cv_text[:200]}..." if len(cv_text) > 200 else f"Content: {cv_text}")
        
        # Step 2: Generate embeddings
        logger.info(f"\nStep 2: Generating embeddings using model: {model_name}")
        try:
            embedder = CVEmbedder(model_name=model_name, logger=logger)
        except Exception as e:
            logger.error(f"Error initializing embedder: {e}")
            sys.exit(1)
        embedding_data = embedder.embed_text(cv_text)
        
        # Add file path to metadata
        embedding_data['cv_file_path'] = cv_path
        
        # Step 3: Save embeddings
        logger.info(f"\nStep 3: Saving embeddings to: {output_path}")
        embedder.save_embedding(embedding_data, output_path)
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUCCESS! CV embeddings generated and saved")
        logger.info("=" * 60)
        logger.info(f"CV File: {cv_path}")
        logger.info(f"Output File: {output_path}")
        logger.info(f"Model: {model_name}")
        logger.info(f"Embedding Dimension: {embedding_data['embedding_dim']}")
        logger.info(f"Text Length: {embedding_data['text_length']} characters")
        logger.info(f"Timestamp: {embedding_data['timestamp']}")
        
    except Exception as e:
        logger.error(f"Error generating embeddings: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

