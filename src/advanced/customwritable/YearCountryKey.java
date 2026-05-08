package advanced.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearCountryKey implements WritableComparable<YearCountryKey> {

    private Text year;
    private Text country;

    public YearCountryKey() {

        this.year = new Text();
        this.country = new Text();
    }

    public YearCountryKey(String year, String country) {

        this.year = new Text(year);
        this.country = new Text(country);
    }

    public String getYear() {
        return year.toString();
    }

    public String getCountry() {
        return country.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {

        year.write(out);
        country.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        year.readFields(in);
        country.readFields(in);
    }

    @Override
    public int compareTo(YearCountryKey other) {

        int compareYear = year.compareTo(other.year);

        if (compareYear != 0) {
            return compareYear;
        }

        return country.compareTo(other.country);
    }

    @Override
    public int hashCode() {

        return year.hashCode() * 163
                + country.hashCode();
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof YearCountryKey) {

            YearCountryKey other = (YearCountryKey) obj;

            return year.equals(other.year)
                    && country.equals(other.country);
        }

        return false;
    }

    @Override
    public String toString() {

        return year.toString()
                + " - "
                + country.toString();
    }
}