all: psguid_map.pdf

psguid_map.dvi: psguid_map.tex
	(latex psguid_map; bibtex psguid_map; makeindex psguid_map; latex psguid_map; latex psguid_map; latex psguid_map)

psguid_map.pdf: psguid_map.dvi
	pdflatex psguid_map

CLEAN=*.dvi *.aux *.log *.toc *.idx *.ilg *.ind *.blg *.bbl *.out
clean: 
	rm -f ${CLEAN}


