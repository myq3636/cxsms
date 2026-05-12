/**
 * Author: frank.xue@King.com
 * Date: 2006-5-9
 * Time: 15:16:40
 * Document Version: 0.1
 */

package com.king.gmms.protocol;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.HashSet;

/**
 * This class is used for mapping GSM 7-bit character to ASCII or Unicode.
 */
public class CharacterMapping {
    private static CharacterMapping instance = new CharacterMapping();

    /**
     * The key of the map is GSM 7-bit character code, the value of the map is ASCII
     * character code. Both of them are not complete which contain erratical mapping
     * only.
     */
    private BidirectionalMap map;

    private CharacterMapping() {
        map = new BidirectionalMap();
        init();
    }

    public static CharacterMapping getInstance() {
        return instance;
    }

    private void init() {
        map.put('\u0000', '\u0040');
        map.put('\u0001', '\u00A3');
        map.put('\u0002', '\u0024');
        map.put('\u0003', '\u00A5');
        map.put('\u0004', '\u00E8');
        map.put('\u0005', '\u00E9');
        map.put('\u0006', '\u00F9');
        map.put('\u0007', '\u00EC');
        map.put('\u0008', '\u00F2');
        map.put('\u0009', '\u00E7');
        map.put('\u000B', '\u00D8');
        map.put('\u000C', '\u00F8');
        map.put('\u000E', '\u00C5');
        map.put('\u000F', '\u00E5');
        map.put('\u0010', '\u0394');
        map.put('\u0011', '\u005F');
        map.put('\u0012', '\u03A6');
        map.put('\u0013', '\u0393');
        map.put('\u0014', '\u039B');
        map.put('\u0015', '\u03A9');
        map.put('\u0016', '\u03A0');
        map.put('\u0017', '\u03A8');
        map.put('\u0018', '\u03A3');
        map.put('\u0019', '\u0398');
        map.put('\u001A', '\u039E');
        map.put('\u001B', '\u00A0');

        map.put('\u1B0A', '\u000C');
        map.put('\u1B14', '\u005E');
        map.put('\u1B28', '\u007B');
        map.put('\u1B29', '\u007D');
        map.put('\u1B3C', '\u005B');
        map.put('\u1B3D', '\u007E');
        map.put('\u1B3E', '\u005D');
        map.put('\u1B40', '\u007C');
        map.put('\u1B65', '\u20AC');

        map.put('\u001C', '\u00C6');
        map.put('\u001D', '\u00E6');
        map.put('\u001E', '\u00DF');
        map.put('\u001F', '\u00C9');
        /**
         * add by brush
         */
        map.put('\u0024','\u00A4');
        map.put('\u0040','\u00A1');
        map.put('\u005C\u005C','\u00D6');
        map.put('\u1B2F','\u005C\u005C');

        map.put('\u005B', '\u00C4');
        map.put('\u005D', '\u00D1');
        map.put('\u005E', '\u00DC');
        map.put('\u005F', '\u00A7');

        map.put('\u0060', '\u00BF');
        map.put('\u007B', '\u00E4');
        map.put('\u007C', '\u00F6');
        map.put('\u007D', '\u00F1');
        map.put('\u007E', '\u00FC');
        map.put('\u007F', '\u00E0');
    }

    /**
     * Map 4-code expanded IRA char to 4-code Unicode char
     *
     * @param original
     * return false or true to judge whether there is none ASCII chars or not.
     */
    public boolean gsm2unicode(char [] original) {
        Set<Character> unicodeChar = new HashSet<Character>();
//        Set<Character> gsmCharCodes = map.keySet();
        for(int i = 0; i < original.length ; i++) {
            char oneChar = original[i];
            if(map.isOrdContains(oneChar)){
                original[i] = map.ordinalGet(oneChar);
            }
//            for(char oneCode : gsmCharCodes) {
//                if(oneChar == oneCode) {
//                    original[i] = map.ordinalGet(oneCode);
//                    break;
//                }
//                if(original[i]>=128)
//                    unicodeChar.add(original[i]);
//            }
            if(original[i]>=128)
                    unicodeChar.add(original[i]);
        }
            return unicodeChar.isEmpty();
    }

    public void unicode2gsm(char [] original) {
//        Set<Character> unicodeCharCodes = map.valueSet();
        for(int i = 0; i < original.length ; i++) {
            char oneChar = original[i];
            if(map.isRevContains(oneChar)){
                original[i] = map.reverseGet(oneChar);
            }
//            for(char oneCode : unicodeCharCodes) {
//                if(oneChar == oneCode) {
//                    original[i] = map.reverseGet(oneCode);
//                }
//            }
        }
    }

    private class BidirectionalMap implements Serializable {
        /*
        * The two LinkedHashMap hold the same data in reverse structure.
        * ordinalMap holds user key for key and user value for value;
        * reverseMap holds user key for value and user value for key.
        */
        private LinkedHashMap<Character, Character> ordinalMap;
        private LinkedHashMap<Character, Character> reverseMap;

        /**
         * Constructs a <code>BidirectionalMap</code>.
         */
        public BidirectionalMap() {
            ordinalMap = new LinkedHashMap<Character, Character>();
            reverseMap = new LinkedHashMap<Character, Character>();
        }

        /**
         * Associates the specified value with the specified key
         * in this map. If the map previously contained the same key
         * or the same value, the old asccociates value or key is replaced.
         *
         * @param key   The key with which the specified value is to be associated.
         * @param value The value to be associated with the specified key.
         */
        public void put(char key, char value) {
            if(ordinalMap.keySet().contains(key)) {
                reverseMap.remove(ordinalMap.get(key));
            }
            if(reverseMap.keySet().contains(value)) {
                ordinalMap.remove(reverseMap.get(value));
            }
            ordinalMap.put(key, value);
            reverseMap.put(value, key);
        }

        /**
         * Gets value from key.
         *
         * @param key The key with which the specified value is to be associated.
         * @return The value to be associated with the specified key.
         */
        public char ordinalGet(char key) {
                return ordinalMap.get(key);
        }

        /**
         * Gets key from value.
         *
         * @param value The value to be associated with the specified key.
         * @return The key with which the specified value is to be associated.
         */
        public char reverseGet(char value) {
            return reverseMap.get(value);
        }

        public boolean isOrdContains(char key){
            return ordinalMap.containsKey(key);
        }

        public boolean isRevContains(char key){
            return reverseMap.containsKey(key);
        }



        /**
         * Returns a set view of the keys contained in this map. The set is backed
         * by the map, so changes to the map are reflected in the set, and vice-versa.
         *
         * @return A set view of the keys contained in this map.
         */
        public Set<Character> keySet() {
            return ordinalMap.keySet();
        }

        /**
         * Returns a set view of the values contained in this map. The set is backed
         * by the map, so changes to the map are reflected in the set, and vice-versa.
         *
         * @return A set view of the values contained in this map.
         */
        public Set<Character> valueSet() {
            return reverseMap.keySet();
        }

        /**
         * Returns the number of key-value mappings in this map.
         *
         * @return The number of key-value mappings in this map.
         */
        public int size() {
            return ordinalMap.size();
        }
    }

    public static void main(String[] args){
        CharacterMapping charactermap = CharacterMapping.getInstance();

        char ch = '\u005C\u005c';
        char chr = charactermap.map.ordinalGet('\u005D');
        System.out.println("chr:" + chr);
        char chr2 = charactermap.map.reverseGet('\u005B');
        System.out.println("chr2:" + chr2);
    }
}
