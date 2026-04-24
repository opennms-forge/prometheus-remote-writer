/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Structural test that catches the class of bug v0.3 shipped: a Java setter
 * was added to {@link PrometheusRemoteWriterConfig} for {@code labels.copy},
 * but the corresponding {@code <property name="labelsCopy" value="${labels.copy}"/>}
 * binding was missing from {@code blueprint.xml} and the {@code <cm:property>}
 * default was missing from the property-placeholder. Every unit test passed
 * because they called the Java setter directly, bypassing the XML path.
 * Operators configuring {@code labels.copy} via their {@code .cfg} file got a
 * silent no-op.
 *
 * <p>This test parses the actual {@code blueprint.xml} and asserts two
 * invariants:
 * <ol>
 *   <li>Every {@code <property name="X" value="${...}"/>} binding on the
 *       {@code config} bean has a corresponding {@code setX(...)} method on
 *       {@link PrometheusRemoteWriterConfig}.</li>
 *   <li>Every {@code <cm:property name="Y">} entry in the property-placeholder
 *       defaults has a matching {@code <property value="${Y}">} binding on the
 *       {@code config} bean (and vice versa) — catches the "added on one side
 *       but not the other" failure mode that was v0.3's specific shape.</li>
 * </ol>
 *
 * <p>A maintainer who adds a new knob without wiring every layer will see a
 * named failure from this test rather than a silent no-op in production.
 */
class BlueprintWiringTest {

    private static final String BLUEPRINT_CLASSPATH = "/OSGI-INF/blueprint/blueprint.xml";

    /** Matches {@code ${foo}} / {@code ${foo.bar}} inside a Blueprint value. */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");

    @Test
    void every_config_bean_property_has_a_matching_java_setter() throws Exception {
        Document doc = loadBlueprint();
        Set<String> beanProps = collectConfigBeanPropertyNames(doc);
        assertThat(beanProps)
            .as("config bean must have at least one <property> binding")
            .isNotEmpty();

        Set<String> setters = collectSetterNames(PrometheusRemoteWriterConfig.class);

        for (String prop : beanProps) {
            String setter = "set" + Character.toUpperCase(prop.charAt(0)) + prop.substring(1);
            assertThat(setters)
                .as("blueprint.xml binds <property name=\"%s\"> but "
                    + "PrometheusRemoteWriterConfig has no %s(...) method — "
                    + "add the setter or remove the binding", prop, setter)
                .contains(setter);
        }
    }

    @Test
    void every_cm_default_is_bound_on_the_config_bean() throws Exception {
        // This is the v0.3-specific failure mode: the Java setter existed but
        // the <cm:property> default AND the <property value="${...}"> binding
        // were both missing. We check both directions so the test fails with
        // a precise name on either side.
        Document doc = loadBlueprint();
        Set<String> cmDefaults = collectCmPropertyNames(doc);
        Set<String> placeholderRefs = collectPlaceholderRefs(doc);

        // Direction 1: every cm:property default should be referenced by a
        // <property value="${…}"> binding.
        Set<String> defaultsMissingBinding = new LinkedHashSet<>(cmDefaults);
        defaultsMissingBinding.removeAll(placeholderRefs);
        assertThat(defaultsMissingBinding)
            .as("<cm:property> default exists but no <property value=\"${X}\"> "
                + "binding references it — operator-facing knob will silently "
                + "no-op. Add the binding on the config bean.")
            .isEmpty();

        // Direction 2: every <property value="${…}"> reference should have a
        // cm:property default (so the placeholder resolves even without a cfg
        // file entry).
        Set<String> refsMissingDefault = new LinkedHashSet<>(placeholderRefs);
        refsMissingDefault.removeAll(cmDefaults);
        assertThat(refsMissingDefault)
            .as("<property value=\"${X}\"> references a placeholder with no "
                + "<cm:property> default — config-placeholder resolution will "
                + "fail at bundle activation unless the operator's cfg file "
                + "defines the key. Add the default.")
            .isEmpty();
    }

    // ---- helpers -----------------------------------------------------------

    private static Document loadBlueprint() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        try (InputStream in = BlueprintWiringTest.class.getResourceAsStream(BLUEPRINT_CLASSPATH)) {
            if (in == null) {
                throw new IllegalStateException(
                    "Could not locate " + BLUEPRINT_CLASSPATH + " on the test classpath. "
                    + "Verify the plugin's resources are compiled and published to the test "
                    + "classpath (Maven should do this automatically).");
            }
            return builder.parse(in);
        }
    }

    /** Collect {@code name} attributes from every {@code <property>} element
     *  (blueprint namespace) nested inside the {@code config} bean. */
    private static Set<String> collectConfigBeanPropertyNames(Document doc) {
        Set<String> out = new LinkedHashSet<>();
        Element configBean = findBeanById(doc, "config");
        NodeList children = configBean.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) continue;
            Element el = (Element) n;
            if ("property".equals(el.getLocalName())) {
                out.add(el.getAttribute("name"));
            }
        }
        return out;
    }

    private static Element findBeanById(Document doc, String id) {
        NodeList beans = doc.getElementsByTagNameNS("*", "bean");
        for (int i = 0; i < beans.getLength(); i++) {
            Element e = (Element) beans.item(i);
            if (id.equals(e.getAttribute("id"))) return e;
        }
        throw new IllegalStateException("No <bean id=\"" + id + "\"> found in blueprint.xml");
    }

    /** Collect {@code name} attributes from every {@code <cm:property>}
     *  element (cm namespace) under the property-placeholder defaults. */
    private static Set<String> collectCmPropertyNames(Document doc) {
        Set<String> out = new LinkedHashSet<>();
        // Namespace-scoped — cm:property lives in the Aries CM namespace, not
        // the default blueprint namespace. `*` wildcard matches both.
        NodeList props = doc.getElementsByTagNameNS("*", "property");
        for (int i = 0; i < props.getLength(); i++) {
            Element el = (Element) props.item(i);
            String ns = el.getNamespaceURI();
            // Only cm:property entries live inside <cm:default-properties>.
            // Filter by namespace to avoid confusion with the bean-level
            // <property> elements the other test counts.
            if (ns != null && ns.contains("blueprint-cm")) {
                String name = el.getAttribute("name");
                if (!name.isEmpty()) out.add(name);
            }
        }
        return out;
    }

    /** Collect every {@code ${X}} referenced by {@code <property value="...">}
     *  on the {@code config} bean. */
    private static Set<String> collectPlaceholderRefs(Document doc) {
        Set<String> out = new LinkedHashSet<>();
        Element configBean = findBeanById(doc, "config");
        NodeList children = configBean.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) continue;
            Element el = (Element) n;
            if (!"property".equals(el.getLocalName())) continue;
            String value = el.getAttribute("value");
            Matcher m = PLACEHOLDER.matcher(value);
            while (m.find()) {
                out.add(m.group(1));
            }
        }
        return out;
    }

    private static Set<String> collectSetterNames(Class<?> clazz) {
        Set<String> out = new HashSet<>();
        for (Method m : clazz.getMethods()) {
            if (m.getName().startsWith("set") && m.getName().length() > 3
                    && m.getParameterCount() == 1) {
                out.add(m.getName());
            }
        }
        return out;
    }
}
