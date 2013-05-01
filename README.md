# flowctrl

Flowctrl (pronouced "flow control") is a proof of concept on how to transform data.
Initially specific to EDI to XML and back again.

Points of interests is to evaluate performance, resource consumption, lines of code, ease of implementing new messages.

## Usage

The data transformation flows is supposed to easily configured. But until that happens the REPL is used for much of the testing :)

Test EDI -> XML -> EDI:

    (def utilmd-xml (xml/emit-str (convert-to-utilmd-xml (parse-by-format (parse-edi (slurp "resources/stamdata.edi")) utilmd-format))))
    
    (edi-to-str (convert-to-utilmde07-edi (xml/parse-str utilmd-xml)))
