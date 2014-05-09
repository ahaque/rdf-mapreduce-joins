package tools;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hbase.util.Bytes;

import javax.xml.bind.DatatypeConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * @author David Alves
 */
public class LUBMDataSetProcessor {

  public static String COLUMN_FAMILY = "o";
  public static byte[] COLUMN_FAMILY_AS_BYTES = Bytes.toBytes(COLUMN_FAMILY);


  public static final String[] URL_PREFIXES = {
	"<http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#",
    "<http://www."
    };

  public static final String[] URL_ABBREVIATIONS = {"rdf_", "ub_", ""};

  public static final Pattern pattern = Pattern.compile("([^\"]\\S*|\".+?\")\\s*");
  public static final Pattern valuePattern = Pattern.compile("^^", Pattern.LITERAL);
  public static final TypeParser defaultParser = new StringParser();

  private static ImmutableMap<String, TypeParser> typesToParsers;

  static {
    ImmutableMap.Builder<String, TypeParser> typesToParsersBuilder = ImmutableMap.builder();
    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#date>", new DateParser());
    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#dateTime>", new DateTimeParser());
    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#string>", new StringParser());
    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#integer>", new IntParser());
    typesToParsersBuilder.put("<http://www4.wiwiss.fu-berlin.de/bizer/LUBM/v01/vocabulary/USD>", new DoubleParser());
    typesToParsers = typesToParsersBuilder.build();
  }

  public static final List<Triple> process(String text) {

    Matcher matcher = pattern.matcher(text);
    if (matcher.groupCount() > 4) {
      throw new IllegalStateException(text);
    }

    matcher.find();
    String subject = matcher.group(1);
    matcher.find();
    String predicate = matcher.group(1);
    matcher.find();

    // if the "object" portion starts with a "<" then it is actually an object, we can return
    String nextComponent = matcher.group(1);
    if (nextComponent.startsWith("<")) {
      return ImmutableList.of(new Triple(subject, predicate, nextComponent));
    }

    String value = nextComponent.replace("\"", "");
    //else it maybe a comment or a value, we treat comments as string values
    matcher.find();
    String next = matcher.group(1);

    ImmutableList.Builder<Triple> builder = ImmutableList.builder();

    if (next.startsWith("@")) {
      builder.add(new Triple(subject, "rdfs_lang", next));
    }

    String[] split = valuePattern.split(next);
    if (split.length == 2) {
      TypeParser parser = typesToParsers.get(split[1]);
      if (parser != null) {
        builder.add(new Triple(subject, predicate, value, parser));
        return builder.build();
      }
    }

    // just treat the value as a string literal if everything else fails
    builder.add(new Triple(subject, predicate, value, defaultParser));
    return builder.build();
  }

  private static String replaceWithShortName(String original) {
    for (int i = 0; i < URL_PREFIXES.length; i++) {
      if (original.contains(URL_PREFIXES[i])) {
        return original.replace(URL_PREFIXES[i], URL_ABBREVIATIONS[i]).replace(">", "");
      }
    }
    return original;
  }

  public static class Triple {

    public final String subject;
    public final String predicate;
    public final String object;
    public final byte[] value;
    public final Object rawValue;

    private Triple(String subject, String predicate, String object) {
      this.subject = replaceWithShortName(subject);
      this.predicate = replaceWithShortName(predicate);
      this.object = replaceWithShortName(object);
      this.value = null;
      this.rawValue = null;
    }

    private Triple(String subject, String predicate, String rawValue, TypeParser parser) {
      this.subject = replaceWithShortName(subject);
      this.predicate = replaceWithShortName(predicate);
      this.object = null;
      this.value = parser.parse(rawValue);
      this.rawValue = rawValue;
    }

    public boolean isValueTriple() {
      return this.value != null;
    }

    public String toString() {
      return Objects.toStringHelper(this)
        .add("subject", subject)
        .add("predicate", predicate)
        .add("object", object)
        .add("rawValue", rawValue)
        .toString();
    }
  }


  private interface TypeParser {
    public byte[] parse(String input);
  }

  private static class IntParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Integer.parseInt(input));
    }
  }

  private static class DoubleParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Double.parseDouble(input));
    }
  }

  @SuppressWarnings("unused")
private static class ShortParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Short.parseShort(input));
    }
  }

  @SuppressWarnings("unused")
private static class FloatParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Float.parseFloat(input));
    }
  }

  @SuppressWarnings("unused")
private static class LongParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Long.parseLong(input));
    }
  }

  @SuppressWarnings("unused")
private static class ByteParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Byte.parseByte(input));
    }
  }

  @SuppressWarnings("unused")
private static class BooleanParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Boolean.parseBoolean(input));
    }
  }

  private static class DateParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(DatatypeConverter.parseDate(input).getTime().getTime());
    }
  }

  private static class DateTimeParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(DatatypeConverter.parseDateTime(input).getTime().getTime());
    }
  }

  private static class StringParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return input.getBytes();
    }
  }

  public static void main(String[] args) throws IOException {
    LUBMDataSetProcessor mapper = new LUBMDataSetProcessor();
    @SuppressWarnings("resource")
	BufferedReader reader = new BufferedReader(new FileReader(args[0]));
    String line;
    while ((line = reader.readLine()) != null) {

      @SuppressWarnings("static-access")
	List<Triple> triples = mapper.process(line);
      for (Triple triple : triples) {
        System.out.println(triple);
      }

    }
  }
}
