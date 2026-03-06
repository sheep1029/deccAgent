# DECC Automation Improvement Plan

This document outlines the planned improvements for the DECC Automation project to enhance code quality, maintainability, and robustness.

## 1. Configuration Management
- [ ] **Externalize Configuration**: Move hardcoded values (like `GATEWAY`, `DECC_V3_CONFIG` base URLs) from `constants.py` and `test_decc_api_full.py` to environment variables or a configuration file (e.g., `.env` or `config.yaml`).
- [ ] **Secure Secrets**: Ensure sensitive information like tokens (e.g., in `test_decc_api_full.py`) are not hardcoded.

## 2. Testing Strategy
- [ ] **Unit Tests**: Implement proper unit tests using `pytest` and `unittest.mock`.
    - Mock API responses for `DECCV3API` and `CoralAPI` to test `DECCFlowV3` logic without network dependency.
    - Test edge cases in `_build_tasks` and `_run_single_task`.
- [ ] **Integration Tests**: Separate the existing `test_decc_api_full.py` into a dedicated integration test suite that runs only when explicitly requested.

## 3. Error Handling & Logging
- [ ] **Specific Exception Handling**: Replace broad `except Exception` blocks with specific exception types (e.g., `requests.exceptions.RequestException`, `ValueError`) to avoid masking unexpected errors.
- [ ] **Structured Logging**: Ensure consistent usage of `logging` across all modules. Add request/response logging (with sensitive data redaction) for better debugging.

## 4. Code Quality & Type Safety
- [ ] **Type Hinting**: comprehensive type annotations for all functions and method arguments/return values.
- [ ] **Static Analysis**: Integrate `mypy` or `pylint` to catch type errors and code style issues.
- [ ] **Refactoring**: 
    - Break down large functions (e.g., `_run_single_task` in `index.py`) into smaller, testable helper functions.
    - Remove unused imports and dead code.

## 5. Documentation
- [ ] **Docstrings**: Add Google-style or Sphinx-style docstrings to all classes and public methods.
- [ ] **README Update**: Update `README.md` with setup instructions, usage examples, and architecture overview.

## 6. CI/CD (Optional)
- [ ] **Automated Checks**: Set up a pre-commit hook or CI pipeline to run tests and linters automatically.
