"""Unit tests for shared.content_type_classifier.

Mirrors entity_extraction_service/tests/test_content_type_classifier.py —
the file was moved to shared/ as part of the hotfix that resolved the
ModuleNotFoundError in code_preprocessor.

Story 4 (Phase 2) — verify that classify_content_type() correctly routes
(file_path, language) pairs to the right content_type string.

Test matrix covers all plan-specified cases plus edge cases for the path
detection logic.
"""

import pytest

from shared.content_type_classifier import (
    classify_content_type,
    _is_ruby_spec,
    _default_content_type_for_language,
)


# ── _is_ruby_spec() ──────────────────────────────────────────────────


class TestIsRubySpec:
    """Unit tests for the internal _is_ruby_spec() helper."""

    def test_spec_rb_suffix(self):
        assert _is_ruby_spec("spec/requests/api/v1/patients_controller_spec.rb") is True

    def test_test_rb_suffix(self):
        assert _is_ruby_spec("test/models/patient_test.rb") is True

    def test_spec_directory_segment(self):
        assert _is_ruby_spec("spec/factories/users.rb") is True

    def test_spec_prefix_relative(self):
        assert _is_ruby_spec("spec/support/shared_contexts.rb") is True

    def test_regular_rb_not_spec(self):
        assert _is_ruby_spec("app/models/patient.rb") is False

    def test_regular_rb_app_services(self):
        assert _is_ruby_spec("app/services/patient_service.rb") is False

    def test_regular_rb_app_controllers(self):
        assert _is_ruby_spec("app/controllers/api/v1/patients_controller.rb") is False

    def test_name_contains_spec_but_not_suffix(self):
        # File named spec_helper.rb in a non-spec directory
        assert _is_ruby_spec("config/spec_helper.rb") is False

    def test_name_ends_with_spec_rb(self):
        # Confirm suffix check on deeply nested path
        assert _is_ruby_spec("engines/billing/spec/requests/invoices_spec.rb") is True

    def test_windows_path_normalised(self):
        # Windows-style path should be normalised
        assert _is_ruby_spec("spec\\requests\\patients_spec.rb") is True

    def test_empty_string(self):
        assert _is_ruby_spec("") is False

    def test_spec_in_middle_of_path(self):
        # /spec/ as a path segment
        assert _is_ruby_spec("engines/billing/spec/factories/clinic.rb") is True


# ── _default_content_type_for_language() ─────────────────────────────


class TestDefaultContentTypeForLanguage:
    """Tests for the language→content_type fallback mapping."""

    def test_ruby_maps_to_ruby_rails(self):
        assert _default_content_type_for_language("Ruby") == "ruby_rails"

    def test_ruby_lowercase(self):
        assert _default_content_type_for_language("ruby") == "ruby_rails"

    def test_typescript(self):
        assert _default_content_type_for_language("TypeScript") == "typescript"

    def test_javascript_maps_to_typescript(self):
        assert _default_content_type_for_language("JavaScript") == "typescript"

    def test_python(self):
        assert _default_content_type_for_language("Python") == "python"

    def test_go(self):
        assert _default_content_type_for_language("Go") == "go"

    def test_unknown_language_lowercased(self):
        assert _default_content_type_for_language("Kotlin") == "kotlin"

    def test_empty_language(self):
        assert _default_content_type_for_language("") == ""


# ── classify_content_type() — plan-specified test cases ──────────────


class TestClassifyContentType:
    """classify_content_type() with plan-specified test cases (Story 4).

    All cases from the plan specification:
    - spec/requests/api/v1/patients_controller_spec.rb, Ruby → ruby_spec
    - app/controllers/patients_controller.rb, Ruby → default (ruby_rails)
    - test/models/patient_test.rb, Ruby → ruby_spec
    - app/services/patient_service.rb, Ruby → default (ruby_rails)
    - TypeScript/Python paths → their existing defaults (unchanged)
    """

    # ── Plan-specified cases ──────────────────────────────────────────

    def test_spec_file_ruby_returns_ruby_spec(self):
        """Plan case 1: spec/requests/*_spec.rb with Ruby → ruby_spec"""
        result = classify_content_type(
            "spec/requests/api/v1/patients_controller_spec.rb",
            "Ruby",
        )
        assert result == "ruby_spec"

    def test_app_controller_ruby_returns_default(self):
        """Plan case 2: app/controllers/*.rb with Ruby → ruby_rails"""
        result = classify_content_type(
            "app/controllers/patients_controller.rb",
            "Ruby",
        )
        assert result == "ruby_rails"

    def test_test_rb_ruby_returns_ruby_spec(self):
        """Plan case 3: test/models/*_test.rb with Ruby → ruby_spec"""
        result = classify_content_type(
            "test/models/patient_test.rb",
            "Ruby",
        )
        assert result == "ruby_spec"

    def test_app_service_ruby_returns_default(self):
        """Plan case 4: app/services/*.rb with Ruby → ruby_rails"""
        result = classify_content_type(
            "app/services/patient_service.rb",
            "Ruby",
        )
        assert result == "ruby_rails"

    def test_typescript_path_returns_typescript(self):
        """Plan case 5a: TypeScript files → typescript (unchanged)"""
        result = classify_content_type(
            "src/components/PatientList.tsx",
            "TypeScript",
        )
        assert result == "typescript"

    def test_python_path_returns_python(self):
        """Plan case 5b: Python files → python (unchanged)"""
        result = classify_content_type(
            "services/patient_service.py",
            "Python",
        )
        assert result == "python"

    # ── Additional Ruby spec coverage ────────────────────────────────

    def test_spec_directory_ruby_returns_ruby_spec(self):
        """Files under spec/ directory (without _spec.rb suffix) are ruby_spec."""
        result = classify_content_type(
            "spec/support/shared_contexts.rb",
            "Ruby",
        )
        assert result == "ruby_spec"

    def test_spec_factories_ruby_returns_ruby_spec(self):
        """Factory files under spec/ are also classified as ruby_spec."""
        result = classify_content_type(
            "spec/factories/users.rb",
            "Ruby",
        )
        assert result == "ruby_spec"

    def test_deeply_nested_spec_rb(self):
        """Deeply nested _spec.rb path returns ruby_spec."""
        result = classify_content_type(
            "spec/requests/api/v1/clinics/soap_notes/workflow_spec.rb",
            "Ruby",
        )
        assert result == "ruby_spec"

    def test_app_model_ruby_returns_default(self):
        """app/models/*.rb → ruby_rails"""
        result = classify_content_type(
            "app/models/patient.rb",
            "Ruby",
        )
        assert result == "ruby_rails"

    def test_initializer_ruby_returns_default(self):
        """config/initializers/*.rb → ruby_rails"""
        result = classify_content_type(
            "config/initializers/devise.rb",
            "Ruby",
        )
        assert result == "ruby_rails"

    # ── Language case-insensitivity ───────────────────────────────────

    def test_ruby_language_case_insensitive(self):
        """Language string is case-insensitive."""
        assert classify_content_type("spec/requests/foo_spec.rb", "ruby") == "ruby_spec"
        assert classify_content_type("spec/requests/foo_spec.rb", "RUBY") == "ruby_spec"
        assert classify_content_type("app/models/foo.rb", "ruby") == "ruby_rails"

    # ── Edge cases ────────────────────────────────────────────────────

    def test_empty_file_path_ruby_returns_default(self):
        """Empty file_path → no spec indicators → ruby_rails."""
        result = classify_content_type("", "Ruby")
        assert result == "ruby_rails"

    def test_empty_language_returns_empty(self):
        """Empty language → no language match → empty fallback."""
        result = classify_content_type("spec/foo_spec.rb", "")
        # language="" → _default_content_type_for_language("") → ""
        # BUT _is_ruby_spec is not called because lang_lower != "ruby"
        assert result == ""

    def test_javascript_not_routed_to_spec(self):
        """Phase 2 does not handle JS spec routing — returns typescript."""
        result = classify_content_type(
            "src/components/__tests__/PatientList.test.tsx",
            "TypeScript",
        )
        assert result == "typescript"  # Not typescript_spec — Phase N

    def test_python_test_not_routed_to_spec(self):
        """Phase 2 does not handle Python test routing — returns python."""
        result = classify_content_type(
            "tests/test_patient_service.py",
            "Python",
        )
        assert result == "python"  # Not python_test — Phase N

    # ── Adelana's 3 dry-run sample files ─────────────────────────────

    def test_sample_workflow_spec(self):
        """Adelana's sample 1: SOAP note workflow spec."""
        result = classify_content_type(
            "spec/requests/api/v1/clinics/soap_notes/workflow_spec.rb",
            "Ruby",
        )
        assert result == "ruby_spec"

    def test_sample_patients_show_spec(self):
        """Adelana's sample 2: Client patients show spec."""
        result = classify_content_type(
            "spec/requests/api/v1/client/patients/show_spec.rb",
            "Ruby",
        )
        assert result == "ruby_spec"

    def test_sample_resend_phone_spec(self):
        """Adelana's sample 3: Resend phone number confirmation spec."""
        result = classify_content_type(
            "spec/requests/api/v1/users/resend_phone_number_confirmation_spec.rb",
            "Ruby",
        )
        assert result == "ruby_spec"
