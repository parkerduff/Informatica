"""
XML Validation Tests for Informatica PowerCenter mapping definitions.

Validates:
- All XML files are well-formed
- XML files have expected root elements (POWERMART)
- Informatica mapping structure (POWERMART > REPOSITORY > FOLDER)
- Required attributes in transformations
- Source/target definitions exist
- Mapping elements present
- Workflow and session definitions
"""
import os
import xml.etree.ElementTree as ET

import pytest

from tests.conftest import (
    ALL_XML_FILES,
    EXPECTED_XML_FILES,
    REPO_ROOT,
    XML_DIR,
)


class TestXMLWellFormed:
    """Verify all XML files in XML/ are well-formed and parseable."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_xml_file_is_well_formed(self, xml_file):
        """Each XML file should be parseable without errors."""
        filepath = os.path.join(XML_DIR, xml_file)
        assert os.path.exists(filepath), f"XML file {xml_file} does not exist"
        try:
            tree = ET.parse(filepath)
            assert tree is not None
        except ET.ParseError as e:
            pytest.fail(f"XML file {xml_file} is not well-formed: {e}")

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_xml_file_not_empty(self, xml_file):
        """Each XML file should have content."""
        filepath = os.path.join(XML_DIR, xml_file)
        file_size = os.path.getsize(filepath)
        assert file_size > 0, f"XML file {xml_file} is empty"

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_xml_has_xml_declaration(self, xml_file):
        """Each XML file should start with an XML declaration."""
        filepath = os.path.join(XML_DIR, xml_file)
        with open(filepath, "r", encoding="latin-1") as f:
            first_line = f.readline().strip()
        assert first_line.startswith("<?xml"), (
            f"XML file {xml_file} missing XML declaration, got: {first_line[:50]}"
        )


class TestXMLRootElements:
    """Verify XML files have expected POWERMART root elements."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_root_element_is_powermart(self, xml_file):
        """Root element should be POWERMART."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        assert root.tag == "POWERMART", (
            f"Expected root element POWERMART, got {root.tag} in {xml_file}"
        )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_powermart_has_creation_date(self, xml_file):
        """POWERMART element should have CREATION_DATE attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        assert "CREATION_DATE" in root.attrib, (
            f"POWERMART missing CREATION_DATE in {xml_file}"
        )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_powermart_has_repository_version(self, xml_file):
        """POWERMART element should have REPOSITORY_VERSION attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        assert "REPOSITORY_VERSION" in root.attrib, (
            f"POWERMART missing REPOSITORY_VERSION in {xml_file}"
        )


class TestInformaticaHierarchy:
    """Validate Informatica mapping structure hierarchy."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_has_repository_element(self, xml_file):
        """Each XML file should contain a REPOSITORY element."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        repos = root.findall("REPOSITORY")
        assert len(repos) > 0, f"No REPOSITORY element found in {xml_file}"

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_repository_has_name(self, xml_file):
        """REPOSITORY element should have NAME attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        repo = root.find("REPOSITORY")
        assert repo is not None
        assert "NAME" in repo.attrib, (
            f"REPOSITORY missing NAME attribute in {xml_file}"
        )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_repository_has_version(self, xml_file):
        """REPOSITORY element should have VERSION attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        repo = root.find("REPOSITORY")
        assert repo is not None
        assert "VERSION" in repo.attrib, (
            f"REPOSITORY missing VERSION attribute in {xml_file}"
        )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_has_folder_element(self, xml_file):
        """Each XML should contain at least one FOLDER element."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        folders = root.findall(".//FOLDER")
        assert len(folders) > 0, f"No FOLDER element found in {xml_file}"

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_folder_has_name(self, xml_file):
        """FOLDER element should have NAME attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        folder = root.find(".//FOLDER")
        assert folder is not None
        assert "NAME" in folder.attrib, (
            f"FOLDER missing NAME attribute in {xml_file}"
        )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_folder_has_owner(self, xml_file):
        """FOLDER element should have OWNER attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        folder = root.find(".//FOLDER")
        assert folder is not None
        assert "OWNER" in folder.attrib, (
            f"FOLDER missing OWNER attribute in {xml_file}"
        )


class TestSourceTargetDefinitions:
    """Verify source and target definitions exist in XML mappings."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_has_source_definitions(self, xml_file):
        """Each XML should contain SOURCE definitions."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        sources = root.findall(".//SOURCE")
        assert len(sources) > 0, f"No SOURCE definitions found in {xml_file}"

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_source_has_name_attribute(self, xml_file):
        """SOURCE elements should have NAME attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        sources = root.findall(".//SOURCE")
        for source in sources:
            assert "NAME" in source.attrib, (
                f"SOURCE missing NAME attribute in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_source_has_databasetype(self, xml_file):
        """SOURCE elements should have DATABASETYPE attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        sources = root.findall(".//SOURCE")
        for source in sources:
            assert "DATABASETYPE" in source.attrib, (
                f"SOURCE missing DATABASETYPE in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_has_target_definitions(self, xml_file):
        """Each XML should contain TARGET definitions."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        targets = root.findall(".//TARGET")
        assert len(targets) > 0, f"No TARGET definitions found in {xml_file}"

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_target_has_name_attribute(self, xml_file):
        """TARGET elements should have NAME attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        targets = root.findall(".//TARGET")
        for target in targets:
            assert "NAME" in target.attrib, (
                f"TARGET missing NAME attribute in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_source_has_sourcefields(self, xml_file):
        """SOURCE elements should contain SOURCEFIELD children."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        sources = root.findall(".//SOURCE")
        for source in sources:
            fields = source.findall("SOURCEFIELD")
            assert len(fields) > 0, (
                f"SOURCE '{source.get('NAME')}' has no SOURCEFIELDs in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_target_has_targetfields(self, xml_file):
        """TARGET elements should contain TARGETFIELD children."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        targets = root.findall(".//TARGET")
        for target in targets:
            fields = target.findall("TARGETFIELD")
            assert len(fields) > 0, (
                f"TARGET '{target.get('NAME')}' has no TARGETFIELDs in {xml_file}"
            )


class TestMappingDefinitions:
    """Verify MAPPING elements in XML files."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_has_mapping_element(self, xml_file):
        """Each XML should contain at least one MAPPING element."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        mappings = root.findall(".//MAPPING")
        assert len(mappings) > 0, f"No MAPPING element found in {xml_file}"

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_mapping_has_name(self, xml_file):
        """MAPPING elements should have NAME attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        mappings = root.findall(".//MAPPING")
        for mapping in mappings:
            assert "NAME" in mapping.attrib, (
                f"MAPPING missing NAME attribute in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_mapping_has_transformations(self, xml_file):
        """MAPPING elements should contain TRANSFORMATION children."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        mappings = root.findall(".//MAPPING")
        for mapping in mappings:
            transforms = mapping.findall("TRANSFORMATION")
            assert len(transforms) > 0, (
                f"MAPPING '{mapping.get('NAME')}' has no TRANSFORMATIONs in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_transformation_has_type(self, xml_file):
        """TRANSFORMATION elements should have TYPE attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        transforms = root.findall(".//TRANSFORMATION")
        for t in transforms:
            assert "TYPE" in t.attrib, (
                f"TRANSFORMATION '{t.get('NAME', 'unknown')}' missing TYPE in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_transformation_has_name(self, xml_file):
        """TRANSFORMATION elements should have NAME attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        transforms = root.findall(".//TRANSFORMATION")
        for t in transforms:
            assert "NAME" in t.attrib, (
                f"TRANSFORMATION missing NAME attribute in {xml_file}"
            )


class TestWorkflowDefinitions:
    """Verify WORKFLOW and SESSION elements where present."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_xml_has_workflow_or_mapping(self, xml_file):
        """Each XML should have either WORKFLOW or MAPPING definitions."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        workflows = root.findall(".//WORKFLOW")
        mappings = root.findall(".//MAPPING")
        assert len(workflows) > 0 or len(mappings) > 0, (
            f"No WORKFLOW or MAPPING found in {xml_file}"
        )

    def test_pay_calendar_has_workflow(self):
        """Pay_Calendar should have a workflow definition."""
        filepath = os.path.join(XML_DIR, "Pay_Calendar")
        tree = ET.parse(filepath)
        root = tree.getroot()
        workflows = root.findall(".//WORKFLOW")
        assert len(workflows) > 0, "Pay_Calendar should contain WORKFLOW"

    def test_comptime_has_workflow(self):
        """COMPTIME should have a workflow definition."""
        filepath = os.path.join(XML_DIR, "COMPTIME")
        tree = ET.parse(filepath)
        root = tree.getroot()
        workflows = root.findall(".//WORKFLOW")
        assert len(workflows) > 0, "COMPTIME should contain WORKFLOW"


class TestXMLFieldAttributes:
    """Verify field-level attributes in source and target definitions."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_sourcefields_have_datatype(self, xml_file):
        """SOURCEFIELD elements should have DATATYPE or FLATFILE attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        fields = root.findall(".//SOURCEFIELD")
        for field in fields:
            # Flat file / COBOL copybook sources may use PHYSICALLENGTH
            # or PICTURETEXT instead of DATATYPE for group items
            has_type_info = (
                "DATATYPE" in field.attrib
                or "PICTURETEXT" in field.attrib
                or "PHYSICALLENGTH" in field.attrib
            )
            assert has_type_info, (
                f"SOURCEFIELD '{field.get('NAME', 'unknown')}' missing type info in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_sourcefields_have_name(self, xml_file):
        """SOURCEFIELD elements should have NAME attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        fields = root.findall(".//SOURCEFIELD")
        for field in fields:
            assert "NAME" in field.attrib, (
                f"SOURCEFIELD missing NAME attribute in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_targetfields_have_datatype(self, xml_file):
        """TARGETFIELD elements should have DATATYPE attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        fields = root.findall(".//TARGETFIELD")
        for field in fields:
            assert "DATATYPE" in field.attrib, (
                f"TARGETFIELD '{field.get('NAME', 'unknown')}' missing DATATYPE in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_sourcefields_have_precision_or_length(self, xml_file):
        """SOURCEFIELD elements should have PRECISION or PHYSICALLENGTH attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        fields = root.findall(".//SOURCEFIELD")
        for field in fields:
            # Flat file sources may use PHYSICALLENGTH instead of PRECISION
            has_size_info = (
                "PRECISION" in field.attrib
                or "PHYSICALLENGTH" in field.attrib
            )
            assert has_size_info, (
                f"SOURCEFIELD '{field.get('NAME', 'unknown')}' missing size info in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_sourcefields_have_fieldnumber(self, xml_file):
        """SOURCEFIELD elements should have FIELDNUMBER attribute."""
        filepath = os.path.join(XML_DIR, xml_file)
        tree = ET.parse(filepath)
        root = tree.getroot()
        fields = root.findall(".//SOURCEFIELD")
        for field in fields:
            assert "FIELDNUMBER" in field.attrib, (
                f"SOURCEFIELD '{field.get('NAME', 'unknown')}' missing FIELDNUMBER in {xml_file}"
            )


class TestPseudossnXML:
    """Specific tests for the Pseudossn XML at repo root (large file)."""

    def test_pseudossn_root_file_exists(self):
        """The Pseudossn file at repo root should exist."""
        filepath = os.path.join(REPO_ROOT, "Pseudossn")
        assert os.path.exists(filepath), "Pseudossn file missing at repo root"

    def test_pseudossn_root_file_is_xml(self):
        """The Pseudossn file at repo root should start with XML declaration."""
        filepath = os.path.join(REPO_ROOT, "Pseudossn")
        with open(filepath, "r", encoding="latin-1") as f:
            first_line = f.readline().strip()
        assert first_line.startswith("<?xml"), (
            "Pseudossn root file should be XML format"
        )

    def test_pseudossn_root_file_has_powermart(self):
        """The Pseudossn root file should have POWERMART root element."""
        filepath = os.path.join(REPO_ROOT, "Pseudossn")
        tree = ET.parse(filepath)
        root = tree.getroot()
        assert root.tag == "POWERMART", (
            f"Pseudossn root file: expected POWERMART, got {root.tag}"
        )
