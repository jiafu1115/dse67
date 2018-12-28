package org.apache.cassandra.schema;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;

public class ViewColumnMetadata {
   protected static final String HIDDEN_COLUMN_PREFIX = ".";
   private final ColumnIdentifier columnName;
   private final AbstractType<?> columnType;
   private final boolean isSelected;
   private final boolean isViewPK;
   private final boolean hasHiddenColumn;
   private final boolean isRegularBaseColumnInViewPrimaryKey;
   private final boolean isFilteredNonPrimaryKey;
   private ColumnMetadata dataColumn;
   private ColumnMetadata hiddenColumn;

   protected ViewColumnMetadata(ColumnIdentifier baseColumnName, AbstractType<?> baseColumnType, boolean isSelected, boolean isFilteredNonPrimaryKey, boolean isBasePK, boolean isViewPK, boolean hasRequiredColumnsForLiveness) {
      this.columnName = baseColumnName;
      this.columnType = baseColumnType;
      this.isSelected = isSelected;
      this.isViewPK = isViewPK;
      this.isRegularBaseColumnInViewPrimaryKey = isViewPK && !isBasePK;
      this.isFilteredNonPrimaryKey = isFilteredNonPrimaryKey;
      this.hasHiddenColumn = this.isRegularBaseColumnInViewPrimaryKey || !isSelected && (isFilteredNonPrimaryKey || !hasRequiredColumnsForLiveness);
   }

   public boolean isRequiredForLiveness() {
      return this.isRegularBaseColumnInViewPrimaryKey || this.isFilteredNonPrimaryKey;
   }

   public boolean isSelected() {
      return this.isSelected;
   }

   public boolean isPrimaryKeyColumn() {
      return this.isViewPK;
   }

   protected boolean isRegularColumn() {
      return this.isSelected && !this.isViewPK;
   }

   protected boolean hasDataColumn() {
      return this.isSelected || this.isViewPK;
   }

   protected boolean hasHiddenColumn() {
      return this.hasHiddenColumn;
   }

   public boolean isFilteredNonPrimaryKey() {
      return this.isFilteredNonPrimaryKey;
   }

   public boolean isRegularBaseColumnInViewPrimaryKey() {
      return this.isRegularBaseColumnInViewPrimaryKey;
   }

   public ColumnIdentifier name() {
      return this.columnName;
   }

   public AbstractType<?> type() {
      return this.columnType;
   }

   public ColumnIdentifier hiddenName() {
      assert this.hasHiddenColumn() : "Should fetch hidden dataColumn name of non-hidden dataColumn.";

      return hiddenViewColumnName(this.columnName);
   }

   protected void setDataColumn(ColumnMetadata column) {
      assert !column.isHidden() : "Use setHiddenColumn instead to add hidden columns";

      this.dataColumn = column;
   }

   protected void setHiddenColumn(ColumnMetadata column) {
      assert this.hasHiddenColumn() && column.isHidden() : "Use setDataColumn instead to add regular columns";

      this.hiddenColumn = column;
   }

   public Cell createCell(Cell baseTableCell) {
      return this.hiddenColumn != null?baseTableCell.withUpdatedColumnNoValue(this.hiddenColumn):baseTableCell.withUpdatedColumn(this.dataColumn);
   }

   public boolean isComplex() {
      return this.getPhysicalColumn().isComplex();
   }

   public ColumnMetadata getPhysicalColumn() {
      assert this.hiddenColumn != null && this.dataColumn != null || !this.isRegularBaseColumnInViewPrimaryKey() : "A data and hidden column can only co-exist for regular base columns in view primary keys.";

      return this.hiddenColumn != null?this.hiddenColumn:this.dataColumn;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         ViewColumnMetadata that = (ViewColumnMetadata)o;
         if(this.isSelected != that.isSelected) {
            return false;
         } else if(this.isViewPK != that.isViewPK) {
            return false;
         } else if(this.hasHiddenColumn != that.hasHiddenColumn) {
            return false;
         } else if(this.isRegularBaseColumnInViewPrimaryKey != that.isRegularBaseColumnInViewPrimaryKey) {
            return false;
         } else if(this.isFilteredNonPrimaryKey != that.isFilteredNonPrimaryKey) {
            return false;
         } else {
            label54: {
               if(this.columnName != null) {
                  if(this.columnName.equals(that.columnName)) {
                     break label54;
                  }
               } else if(that.columnName == null) {
                  break label54;
               }

               return false;
            }

            if(this.dataColumn != null) {
               if(this.dataColumn.equals(that.dataColumn)) {
                  return this.hiddenColumn != null?this.hiddenColumn.equals(that.hiddenColumn):that.hiddenColumn == null;
               }
            } else if(that.dataColumn == null) {
               return this.hiddenColumn != null?this.hiddenColumn.equals(that.hiddenColumn):that.hiddenColumn == null;
            }

            return false;
         }
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.columnName != null?this.columnName.hashCode():0;
      result = 31 * result + (this.isSelected?1:0);
      result = 31 * result + (this.isViewPK?1:0);
      result = 31 * result + (this.hasHiddenColumn?1:0);
      result = 31 * result + (this.isRegularBaseColumnInViewPrimaryKey?1:0);
      result = 31 * result + (this.isFilteredNonPrimaryKey?1:0);
      result = 31 * result + (this.dataColumn != null?this.dataColumn.hashCode():0);
      result = 31 * result + (this.hiddenColumn != null?this.hiddenColumn.hashCode():0);
      return result;
   }

   public String toString() {
      return "ViewColumnMetadata{columnName=" + this.columnName + ", isSelected=" + this.isSelected + ", isViewPK=" + this.isViewPK + ", hasHiddenColumn=" + this.hasHiddenColumn + ", isRegularBaseColumnInViewPrimaryKey=" + this.isRegularBaseColumnInViewPrimaryKey + ", isFilteredNonPrimaryKey=" + this.isFilteredNonPrimaryKey + ", dataColumn=" + this.dataColumn + ", hiddenColumn=" + this.hiddenColumn + '}';
   }

   @VisibleForTesting
   public static ColumnIdentifier hiddenViewColumnName(ColumnIdentifier baseColumnName) {
      return ColumnIdentifier.getInterned(String.format("%s%s", new Object[]{".", baseColumnName.toString()}), true);
   }
}
