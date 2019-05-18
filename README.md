## Standard Product Completeness Evaluator

Evaluates S1-GUNW, S1-GUNW-MERGED, or AOIs along track and date for completeness. If all products are complete, it publishes AOI_TRACK products

There are two associated jobs:
- Standard Product S1-GUNW - AOI Completeness Evaluator
- Standard Product S1-GUNW - S1-GUNW Completeness Evaluator


### Standard Product S1-GUNW - AOI Completeness Evaluator
-----
Iterative job. Input is an AOI. There are no other inputs. Queries for all intermediate products, GUNWs, and GUNW-MERGED associated with the AOI, then iterates through all the GUNWs and GUNW-MERGED, publishing AOI_TRACK products for complete tracks for a given orbit pairing. Additionally, when complete it will tag GUNW and GUNW-MERGED with the approprate AOI_name machine tag (this may be multiple AOIs). Will also tag acq-lists with "gunw_complete" or "gunw_missing" depending on whether the GUNW has been generated.



### Standard Product S1-GUNW - S1-GUNW Completeness Evaluator
-----

Iterative job. Input is a GUNW or GUNW-MERGED. There are no other inputs. Queries for aois, intermediate products, and co-located GUNW/GUNW-MERGED associated with the input GUNW. Determines whether any covered AOIs are complete along track & orbit pairing. If they are complete, it publishes AOI_TRACK products for the complete track.  Additionally, when complete it will tag GUNW and GUNW-MERGED with the approprate AOI_name machine tag (this may be multiple AOIs). Will also tag acq-lists with "gunw_complete" or "gunw_missing" depending on whether the GUNW has been generated.
