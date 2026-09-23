/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Architectural rules enforced mechanically via ArchUnit.
 * Ensures Iceberg and Delta table format packages remain isolated.
 */
@AnalyzeClasses(
        packages = "io.lakestream.ursa.lakehouse",
        importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule ICEBERG_DOES_NOT_DEPEND_ON_DELTA = noClasses()
            .that().resideInAPackage("io.lakestream.ursa.lakehouse.iceberg..")
            .should().dependOnClassesThat()
            .resideInAPackage("io.lakestream.ursa.lakehouse.delta..")
            .because("Iceberg and Delta packages must remain isolated");

    @ArchTest
    static final ArchRule DELTA_DOES_NOT_DEPEND_ON_ICEBERG = noClasses()
            .that().resideInAPackage("io.lakestream.ursa.lakehouse.delta..")
            .should().dependOnClassesThat()
            .resideInAPackage("io.lakestream.ursa.lakehouse.iceberg..")
            .because("Delta and Iceberg packages must remain isolated");
}
