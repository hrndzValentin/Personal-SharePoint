<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="3.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                exclude-result-prefixes="xs">

  <xsl:output method="json" indent="no"/>

  <xsl:template match="/PARTY">
    <xsl:map>
      <!-- Todos los atributos de PARTY, genéricamente -->
      <xsl:for-each select="@*">
        <xsl:map-entry key="local-name()" select="string(.)"/>
      </xsl:for-each>

      <!-- Asociaciones -->
      <xsl:map-entry key="'associations'">
        <xsl:sequence select="array {
          for $a in PARTYASSC return map {
            'assctype':  string($a/@assctype),
            'asscparty': string($a/@asscparty)
          }
        }"/>
      </xsl:map-entry>

      <!-- Clasificaciones -->
      <xsl:map-entry key="'classifications'">
        <xsl:sequence select="array {
          for $c in PARTYCLASS return map {
            'class': string($c/@class),
            'code':  string($c/@code)
          }
        }"/>
      </xsl:map-entry>

      <!-- Extensiones -->
      <xsl:map-entry key="'extensions'">
        <xsl:sequence select="array {
          for $e in PARTYEXT return map {
            'service': string($e/@service),
            'extref':  string($e/@extref)
          }
        }"/>
      </xsl:map-entry>
    </xsl:map>
  </xsl:template>

</xsl:stylesheet>
