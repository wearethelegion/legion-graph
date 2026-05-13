"""
Minimal canonical path builder for human-readable debugging paths.

Format: workspace::repository::branch::filepath#symbol
Example: default::oscar_vet__vet_backend::develop::app__controllers__api_controller_rb#update_status
"""


class CanonicalPath:
    """Build human-readable hierarchical paths for debugging and logging."""

    @staticmethod
    def normalize(text: str) -> str:
        """
        Normalize text to canonical format.

        Rules:
            - Replace / with __
            - Replace . with __
            - Replace - with _
            - Convert to lowercase

        Example:
            >>> CanonicalPath.normalize("oscar-vet/vet_backend")
            'oscar_vet__vet_backend'
        """
        return (text.strip()
                .replace("/", "__")
                .replace(".", "__")
                .replace("-", "_")
                .lower())

    @staticmethod
    def build_document_path(
        workspace: str,
        repository: str,
        branch: str,
        filepath: str
    ) -> str:
        """
        Build document path from location components.

        Format: workspace::repository::branch::filepath

        Example:
            >>> CanonicalPath.build_document_path(
            ...     "default",
            ...     "oscar-vet/vet_backend",
            ...     "develop",
            ...     "app/controllers/api_controller.rb"
            ... )
            'default::oscar_vet__vet_backend::develop::app__controllers__api_controller_rb'
        """
        return f"{workspace.strip()}::{CanonicalPath.normalize(repository)}::{branch.strip()}::{CanonicalPath.normalize(filepath)}"

    @staticmethod
    def build_symbol_path(document_path: str, symbol_name: str) -> str:
        """
        Extend document path with symbol name.

        Format: document_path#symbol_name

        Example:
            >>> doc = "default::myrepo::main::src__utils_rb"
            >>> CanonicalPath.build_symbol_path(doc, "parse_data")
            'default::myrepo::main::src__utils_rb#parse_data'
        """
        return f"{document_path}#{symbol_name.strip()}"
