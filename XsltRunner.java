@Component
public class XsltRunner implements CommandLineRunner {

    @Override
    public void run(String... args) throws Exception {
        Processor processor = new Processor(false);
        XsltCompiler compiler = processor.newXsltCompiler();
        XsltExecutable executable = compiler.compile(
                new StreamSource(getClass().getResourceAsStream("/Party.xslt")));

        XMLInputFactory xif = XMLInputFactory.newInstance();
        xif.setProperty(XMLInputFactory.IS_COALESCING, true);
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        xif.setProperty("javax.xml.stream.isSupportingExternalEntities", false);

        try (InputStream in  = getClass().getResourceAsStream("/EF_PARTY_A.01Jun2026.xml");
             OutputStream out = Files.newOutputStream(Path.of("parties.jsonl"))) {

            XMLEventReader reader = xif.createXMLEventReader(in);
            DocumentBuilder builder = processor.newDocumentBuilder();

            while (reader.hasNext()) {
                XMLEvent peek = reader.peek();
                if (peek.isStartElement()
                        && "PARTY".equals(peek.asStartElement().getName().getLocalPart())) {

                    String partyXml = readSubtree(reader);
                    XdmNode partyNode = builder.build(
                            new StreamSource(new StringReader(partyXml)));

                    Xslt30Transformer transformer = executable.load30();
                    Serializer serializer = processor.newSerializer(out);
                    transformer.applyTemplates(partyNode, serializer);
                    out.write('\n');
                } else {
                    reader.nextEvent();
                }
            }
            reader.close();
        }
    }

    /** Consume eventos desde el START_ELEMENT actual hasta su END_ELEMENT
     *  balanceado, serializándolos a un String XML bien formado. */
    private static String readSubtree(XMLEventReader reader) throws Exception {
        StringWriter sw = new StringWriter(4096);
        int depth = 0;
        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            event.writeAsEncodedUnicode(sw);
            if (event.isStartElement()) {
                depth++;
            } else if (event.isEndElement()) {
                depth--;
                if (depth == 0) return sw.toString();
            }
        }
        throw new IllegalStateException("PARTY sin cierre");
    }
}
