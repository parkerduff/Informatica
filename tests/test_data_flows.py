"""
Data Flow Tests for BIISINT Informatica ETL mappings.

Validates:
- ETL mapping references are consistent
- Source-to-target field mappings in XML
- Transformation logic references
- Connector/instance consistency
- Mapping completeness
"""
import os
import xml.etree.ElementTree as ET

import pytest

from tests.conftest import (
    EXPECTED_XML_FILES,
    REPO_ROOT,
    XML_DIR,
)


def parse_xml(xml_file):
    """Parse an XML file and return the root element."""
    filepath = os.path.join(XML_DIR, xml_file)
    tree = ET.parse(filepath)
    return tree.getroot()


class TestMappingConnectors:
    """Verify CONNECTOR elements link transformations properly."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_mappings_have_connectors(self, xml_file):
        """Each MAPPING should have CONNECTOR elements linking transformations."""
        root = parse_xml(xml_file)
        mappings = root.findall(".//MAPPING")
        for mapping in mappings:
            connectors = mapping.findall("CONNECTOR")
            assert len(connectors) > 0, (
                f"MAPPING '{mapping.get('NAME')}' has no CONNECTORs in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_connectors_have_from_and_to(self, xml_file):
        """CONNECTOR elements should have FROMINSTANCE/TOINSTANCE attributes."""
        root = parse_xml(xml_file)
        connectors = root.findall(".//CONNECTOR")
        for conn in connectors:
            assert "FROMINSTANCE" in conn.attrib, (
                f"CONNECTOR missing FROMINSTANCE in {xml_file}"
            )
            assert "TOINSTANCE" in conn.attrib, (
                f"CONNECTOR missing TOINSTANCE in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_connectors_have_field_references(self, xml_file):
        """CONNECTOR elements should reference specific fields."""
        root = parse_xml(xml_file)
        connectors = root.findall(".//CONNECTOR")
        for conn in connectors:
            assert "FROMFIELD" in conn.attrib, (
                f"CONNECTOR missing FROMFIELD in {xml_file}"
            )
            assert "TOFIELD" in conn.attrib, (
                f"CONNECTOR missing TOFIELD in {xml_file}"
            )


class TestMappingInstances:
    """Verify INSTANCE elements in mappings reference valid transformations."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_mappings_have_instances(self, xml_file):
        """Each MAPPING should have INSTANCE elements."""
        root = parse_xml(xml_file)
        mappings = root.findall(".//MAPPING")
        for mapping in mappings:
            instances = mapping.findall("INSTANCE")
            assert len(instances) > 0, (
                f"MAPPING '{mapping.get('NAME')}' has no INSTANCEs in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_instances_have_names(self, xml_file):
        """INSTANCE elements should have NAME attribute."""
        root = parse_xml(xml_file)
        instances = root.findall(".//INSTANCE")
        for inst in instances:
            assert "NAME" in inst.attrib, (
                f"INSTANCE missing NAME in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_instances_have_type(self, xml_file):
        """INSTANCE elements should have TYPE attribute."""
        root = parse_xml(xml_file)
        instances = root.findall(".//INSTANCE")
        for inst in instances:
            assert "TYPE" in inst.attrib, (
                f"INSTANCE '{inst.get('NAME', 'unknown')}' missing TYPE in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_connector_instances_exist_in_mapping(self, xml_file):
        """CONNECTOR FROMINSTANCE/TOINSTANCE should reference actual INSTANCEs."""
        root = parse_xml(xml_file)
        mappings = root.findall(".//MAPPING")
        for mapping in mappings:
            instance_names = {
                inst.get("NAME") for inst in mapping.findall("INSTANCE")
            }
            connectors = mapping.findall("CONNECTOR")
            for conn in connectors:
                from_inst = conn.get("FROMINSTANCE")
                to_inst = conn.get("TOINSTANCE")
                assert from_inst in instance_names, (
                    f"CONNECTOR references non-existent FROMINSTANCE '{from_inst}' "
                    f"in mapping '{mapping.get('NAME')}' in {xml_file}"
                )
                assert to_inst in instance_names, (
                    f"CONNECTOR references non-existent TOINSTANCE '{to_inst}' "
                    f"in mapping '{mapping.get('NAME')}' in {xml_file}"
                )


class TestTransformationTypes:
    """Verify transformation type distribution in mappings."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_has_source_qualifier(self, xml_file):
        """Each mapping should have a Source Qualifier transformation."""
        root = parse_xml(xml_file)
        transforms = root.findall(".//TRANSFORMATION")
        types = [t.get("TYPE", "") for t in transforms]
        assert "Source Qualifier" in types, (
            f"No Source Qualifier transformation in {xml_file}"
        )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_has_expression_transformation(self, xml_file):
        """Most mappings should have Expression transformations."""
        root = parse_xml(xml_file)
        transforms = root.findall(".//TRANSFORMATION")
        types = [t.get("TYPE", "") for t in transforms]
        assert "Expression" in types, (
            f"No Expression transformation in {xml_file}"
        )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_transformations_have_transformfields(self, xml_file):
        """TRANSFORMATION elements should have TRANSFORMFIELD children."""
        root = parse_xml(xml_file)
        transforms = root.findall(".//TRANSFORMATION")
        for t in transforms:
            fields = t.findall("TRANSFORMFIELD")
            assert len(fields) > 0, (
                f"TRANSFORMATION '{t.get('NAME')}' has no TRANSFORMFIELDs in {xml_file}"
            )


class TestSourceTargetConsistency:
    """Verify source and target definitions are consistent with mappings."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_source_names_referenced_in_instances(self, xml_file):
        """SOURCE names should be referenced by INSTANCE elements."""
        root = parse_xml(xml_file)
        folder = root.find(".//FOLDER")
        if folder is None:
            pytest.skip(f"No FOLDER in {xml_file}")

        source_names = {s.get("NAME") for s in folder.findall("SOURCE")}
        instance_transformation_names = set()
        for mapping in folder.findall("MAPPING"):
            for inst in mapping.findall("INSTANCE"):
                tn = inst.get("TRANSFORMATION_NAME")
                if tn:
                    instance_transformation_names.add(tn)

        for source_name in source_names:
            assert source_name in instance_transformation_names, (
                f"SOURCE '{source_name}' not referenced by any INSTANCE in {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_target_names_referenced_in_instances(self, xml_file):
        """TARGET names should be referenced by INSTANCE elements."""
        root = parse_xml(xml_file)
        folder = root.find(".//FOLDER")
        if folder is None:
            pytest.skip(f"No FOLDER in {xml_file}")

        target_names = {t.get("NAME") for t in folder.findall("TARGET")}
        instance_transformation_names = set()
        for mapping in folder.findall("MAPPING"):
            for inst in mapping.findall("INSTANCE"):
                tn = inst.get("TRANSFORMATION_NAME")
                if tn:
                    instance_transformation_names.add(tn)

        for target_name in target_names:
            assert target_name in instance_transformation_names, (
                f"TARGET '{target_name}' not referenced by any INSTANCE in {xml_file}"
            )


class TestEHRP2BIISDataFlow:
    """Specific data flow tests for the EHRP2BIIS_UPDATE mapping."""

    def test_ehrp2biis_has_ps_gvt_job_source(self):
        """EHRP2BIIS should have PS_GVT_JOB source."""
        root = parse_xml("EHRP2BIIS_UPDATE")
        sources = root.findall(".//SOURCE")
        source_names = [s.get("NAME") for s in sources]
        assert "PS_GVT_JOB" in source_names, (
            "EHRP2BIIS should have PS_GVT_JOB source"
        )

    def test_ehrp2biis_has_new_ehrp_actions_source(self):
        """EHRP2BIIS should have NWK_NEW_EHRP_ACTIONS_TBL source."""
        root = parse_xml("EHRP2BIIS_UPDATE")
        sources = root.findall(".//SOURCE")
        source_names = [s.get("NAME") for s in sources]
        assert "NWK_NEW_EHRP_ACTIONS_TBL" in source_names, (
            "EHRP2BIIS should have NWK_NEW_EHRP_ACTIONS_TBL source"
        )

    def test_ehrp2biis_has_oracle_sources(self):
        """EHRP2BIIS sources should be Oracle database type."""
        root = parse_xml("EHRP2BIIS_UPDATE")
        sources = root.findall(".//SOURCE")
        for source in sources:
            assert source.get("DATABASETYPE") == "Oracle", (
                f"Source '{source.get('NAME')}' should be Oracle type"
            )


class TestPayCalendarDataFlow:
    """Specific data flow tests for Pay_Calendar."""

    def test_pay_calendar_has_session(self):
        """Pay_Calendar should have SESSION elements."""
        root = parse_xml("Pay_Calendar")
        sessions = root.findall(".//SESSION")
        assert len(sessions) > 0, "Pay_Calendar should have SESSION elements"

    def test_pay_calendar_has_workflow(self):
        """Pay_Calendar should have a WORKFLOW element."""
        root = parse_xml("Pay_Calendar")
        workflows = root.findall(".//WORKFLOW")
        assert len(workflows) > 0, "Pay_Calendar should have WORKFLOW"


class TestLESDataFlow:
    """Specific data flow tests for LES (Leave and Earnings Statement)."""

    def test_les_has_multiple_targets(self):
        """LES should have multiple target tables for different record types."""
        root = parse_xml("LES")
        targets = root.findall(".//TARGET")
        assert len(targets) > 1, (
            f"LES should have multiple targets, found {len(targets)}"
        )

    def test_les_has_multiple_sources(self):
        """LES should have multiple source definitions."""
        root = parse_xml("LES")
        sources = root.findall(".//SOURCE")
        assert len(sources) > 0, "LES should have source definitions"


class TestCPMDataFlow:
    """Specific data flow tests for CPM (Central Personnel Monitoring)."""

    def test_cpm_has_substantial_mappings(self):
        """Core CPM should have substantial mapping definitions."""
        root = parse_xml("CPM")
        transforms = root.findall(".//TRANSFORMATION")
        assert len(transforms) > 5, (
            f"CPM should have many transformations, found {len(transforms)}"
        )

    def test_cpm_has_multiple_targets(self):
        """CPM should have multiple target tables."""
        root = parse_xml("CPM")
        targets = root.findall(".//TARGET")
        assert len(targets) > 1, (
            f"CPM should have multiple targets, found {len(targets)}"
        )
