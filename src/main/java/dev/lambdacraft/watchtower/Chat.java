package dev.lambdacraft.watchtower;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import net.minecraft.entity.player.PlayerEntity;
import net.minecraft.text.LiteralTextContent;
import net.minecraft.text.Style;
import net.minecraft.text.Text;
import net.minecraft.text.MutableText;
import net.minecraft.util.Formatting;
import net.minecraft.util.Identifier;
import net.minecraft.util.math.BlockPos;

import static net.minecraft.util.Formatting.*;

/**
 * Utility class to make sending chat messages easier
 */
public class Chat {
  private static final HashMap<Class<?>, Formatting> textColorMap;
  private static final DateTimeFormatter dtf;
  private static final ZoneId zoneId;

  static {
    textColorMap = new HashMap<>();
    textColorMap.put(Instant.class, GRAY);
    textColorMap.put(Identifier.class, YELLOW);
    textColorMap.put(Integer.class, GOLD);
    dtf = DateTimeFormatter.ofPattern("dd/MM/yy HH:mm:ss");
    zoneId = ZoneId.of("UTC");
  }
  public static void send(PlayerEntity p, String s) {
    p.sendMessage(text(s), false);
  }

  public static void send(PlayerEntity p, Text text) {
    p.sendMessage(text, false);
  }

  public static void send(PlayerEntity p, MutableText... texts) {
    p.sendMessage(joinText(texts), false);
  }

  public static MutableText text(String s) {
    return MutableText.of(new LiteralTextContent(s));
  }

  public static MutableText format(MutableText t, Formatting f) {
    return t.setStyle(Style.EMPTY.withFormatting(f));
  }

  public static MutableText format(String s, Formatting f) {
    return format(text(s), f);
  }

  public static MutableText format(Object o) {
    if (textColorMap.containsKey(o.getClass())) {
      return format(stringFrom(o), textColorMap.get(o.getClass()));
    }
    return text(stringFrom(o));
  }

  public static MutableText joinText(MutableText... texts) {
    MutableText out = text("");
    for (MutableText text : texts) {
      out.append(" ").append(text);
    }
    return out;
  }

  public static MutableText concat(String sep, MutableText t1, MutableText t2) {
    t1.append(text(sep)).append(t2);
    return t1;
  }

  public static String stringFrom(Object o) {
    if (o.getClass() == BlockPos.class) {
      BlockPos pos = (BlockPos)o;
      return "(" + pos.getX() + "," + pos.getY() + "," + pos.getZ() + ")";
    }
    if (o.getClass() == Instant.class) {
      Instant instant = (Instant)o;
      ZonedDateTime dateTimeInTz = ZonedDateTime.ofInstant(instant, zoneId);

      return dateTimeInTz.format(dtf);
    }
    if (o.getClass() == Identifier.class) {
      Identifier id = (Identifier)o;
      return id.getNamespace().equals("minecraft") ? id.getPath() : id.toString();
    }
    return o.toString();
  }
}