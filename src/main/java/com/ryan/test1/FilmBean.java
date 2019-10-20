package com.ryan.test1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FilmBean implements WritableComparable<FilmBean> {

    private String category;
    private String filmname;
    private int filmtime;

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getFilmname() {
        return filmname;
    }

    public void setFilmname(String filmname) {
        this.filmname = filmname;
    }

    public int getFilmtime() {
        return filmtime;
    }

    public void setFilmtime(int filmtime) {
        this.filmtime = filmtime;
    }

    public void set(String category, String filmname, int filmtime) {
        this.category = category;
        this.filmname = filmname;
        this.filmtime = filmtime;
    }

    @Override
    public String toString() {
        return category + "\t" + filmname + "\t" + filmtime;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(category);
        dataOutput.writeUTF(filmname);
        dataOutput.writeInt(filmtime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.category = dataInput.readUTF();
        this.filmname = dataInput.readUTF();
        this.filmtime = dataInput.readInt();
    }

    /**
     * 比较规则
     * @param o
     * @return 先比较类型，如果相等就比较年份，不等就按照类型排序
     */
    @Override
    public int compareTo(FilmBean o) {
        int compare = this.category.compareTo(o.category);

        if (compare == 0) {
            return Integer.compare(o.filmtime, this.filmtime);
        }else {
            return compare;
        }
    }
}
