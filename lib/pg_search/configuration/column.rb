require 'digest'

module PgSearch
  class Configuration
    class Column
      TIMESTAMP_FORMAT = "YYYYMMDDHH24MISS"

      attr_reader :weight, :association

      def initialize(column_name, weight, model, association = nil)
        @column_name = column_name.to_s
        @column_type = model.columns.detect { |column| column.name == column_name.to_s }.instance_variable_get("@type") # Suppress .type warning
        @weight = weight
        @model = model
        @association = association
      end

      def table
        foreign? ? @association.table_name : @model.table_name
      end

      def full_name
        "#{@model.connection.quote_table_name(table)}.#{@model.connection.quote_column_name(@column_name)}"
      end

      def to_sql
        name = if foreign?
                 "#{@association.subselect_alias}.#{self.alias}"
               else
                 full_name
               end
        case @column_type
        when :date, :datetime
          "coalesce(to_char(#{name}, '#{TIMESTAMP_FORMAT}'), '')"
        else
          "coalesce(#{name}, '')"
        end
      end

      def foreign?
        @association.present?
      end

      def alias
        Configuration.alias(association.subselect_alias, @column_name)
      end
    end
  end
end
